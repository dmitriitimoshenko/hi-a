package tgbt

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"time"

	kafkamessages "github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/kafka/handlers/messages"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/services/dto"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/tools"
	tgbot "github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
)

const (
	errorMessage          = "⭕ INTERNAL ERROR OCCURED ⭕"
	confirmPopUpMessage   = "☑️ Confirming..."
	confirmMessage        = "✅ Confirmed"
	detailsMissingMessage = "ℹ️ Details for this event are not available anymore"
	detailsCommingMessage = "☑️ Details will be sent to you shortly"

	KAFKA_TOPIC_APPLICATION_UPDATE_UNPROCESSED = "KAFKA_TOPIC_APPLICATION_UPDATE_UNPROCESSED"
	KAFKA_TOPIC_FEEDBACK                       = "KAFKA_TOPIC_FEEDBACK"
	applicationUpdateUnprocessedConfirmKey     = "cnfm_button_pressed"

	detailsCacheRefreshTTL     = 31 * 24 * time.Hour
	skipReasonSelectedCacheTTL = 31 * 24 * time.Hour
)

type redisClient interface {
	Set(ctx context.Context, key string, value string, expiration time.Duration) (bool, error)
	Get(ctx context.Context, key string) (string, bool, error)
	Delete(ctx context.Context, key string) (int64, error)
}

type kafkaClient interface {
	Publish(ctx context.Context, topic string, key []byte, value []byte) error
}

type TelegramBotHandler struct {
	logger      *slog.Logger
	redisClient redisClient
	kafkaClient kafkaClient
}

func NewTelegramBotHandler(
	logger *slog.Logger,
	redisClient redisClient,
	kafkaClient kafkaClient,
) *TelegramBotHandler {
	return &TelegramBotHandler{
		logger:      logger,
		redisClient: redisClient,
		kafkaClient: kafkaClient,
	}
}

func (h *TelegramBotHandler) Handle(ctx context.Context, b *tgbot.Bot, update *models.Update) {
	switch {
	case update.CallbackQuery != nil:
		data := update.CallbackQuery.Data

		switch {
		case strings.HasPrefix(data, "skipreason:"):
			h.handleSkipReason(ctx, b, update)
			return
		}

		dataParts := strings.Split(data, ":")
		if len(dataParts) < 3 {
			h.logger.Error("invalid callback data format", slog.String("data", data))
			return
		}

		emailLabel := enums.EmailLabel(dataParts[0])
		if !emailLabel.IsValid() {
			h.logger.Error("unsupported callback prefix", slog.String("prefix", string(emailLabel)))
			return
		}

		action := dataParts[1]
		emailID := dataParts[len(dataParts)-1]
		switch action {
		case "cnfm":
			h.handleMappingConfirmation(ctx, b, update, emailLabel, emailID)
		case "dtls":
			h.handleMappingDetails(ctx, b, update, emailLabel, emailID)
		case "skp":
			h.handleMappingSkip(ctx, b, update, emailLabel, emailID)
		default:
			h.logger.Error("unknown callback action", slog.String("action", action))
		}
	}
}

func (h *TelegramBotHandler) handleMappingConfirmation(
	ctx context.Context,
	b *tgbot.Bot,
	update *models.Update,
	emailLabel enums.EmailLabel,
	emailID string,
) {
	if emailID == "" {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleMappingConfirmation] emailID is empty", "label", emailLabel)
		return
	}

	callbackMessage := update.CallbackQuery.Message.Message
	if callbackMessage == nil {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleMappingConfirmation] callback message is nil", "label", emailLabel)
		return
	}

	cacheKey := fmt.Sprintf("%d:%s", update.CallbackQuery.From.ID, emailID)
	val, ok, err := h.redisClient.Get(ctx, cacheKey)
	if err != nil {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleMappingConfirmation] failed to get from redis", "err", err)
		return
	}
	if !ok {
		ok, err = b.AnswerCallbackQuery(ctx, &tgbot.AnswerCallbackQueryParams{
			CallbackQueryID: update.CallbackQuery.ID,
			Text:            detailsMissingMessage,
			ShowAlert:       false,
		})
		if err != nil {
			h.logger.Error("[handleMappingConfirmation] failed to answer callback query", "err", err)
			h.notifyInternalError(ctx, b, update)
			return
		}
		if !ok {
			h.logger.Error("[handleMappingConfirmation] failed to answer callback query: not ok")
			h.notifyInternalError(ctx, b, update)
			return
		}
	}

	ok, err = b.AnswerCallbackQuery(ctx, &tgbot.AnswerCallbackQueryParams{
		CallbackQueryID: update.CallbackQuery.ID,
		Text:            confirmPopUpMessage,
		ShowAlert:       false,
	})
	if err != nil {
		h.logger.Error("[handleMappingConfirmation] failed to answer callback query", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}
	if !ok {
		h.logger.Error("[handleMappingConfirmation] failed to answer callback query: not ok")
		h.notifyInternalError(ctx, b, update)
		return
	}

	var payload kafkamessages.NotificationMessage
	if err = json.Unmarshal([]byte(val), &payload); err != nil {
		h.logger.Error("[handleMappingConfirmation] failed to unmarshal payload before publish", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}

	payload.Action = "cnfm"

	confirmPayload, err := json.Marshal(payload)
	if err != nil {
		h.logger.Error("[handleMappingConfirmation] failed to marshal payload before publish", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}

	topicToPublish := os.Getenv("KAFKA_TOPIC_APPLICATION_UPDATE_UNPROCESSED")
	if err = h.kafkaClient.Publish(ctx, topicToPublish, []byte(applicationUpdateUnprocessedConfirmKey), confirmPayload); err != nil {
		h.logger.Error("[handleMappingConfirmation] failed to publish to kafka", "err", err, "label", emailLabel)
		h.notifyInternalError(ctx, b, update)
		return
	}

	if _, err = h.redisClient.Delete(ctx, cacheKey); err != nil {
		h.logger.Error("[handleMappingConfirmation] failed to delete from redis", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}

	if err = h.removeInlineKeyboard(ctx, b, callbackMessage); err != nil {
		h.logger.Error("[handleMappingConfirmation] failed to remove inline keyboard", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}

	replyMessage := callbackMessage.ReplyToMessage
	if err = h.appendLineToMessage(ctx, b, confirmMessage, replyMessage); err != nil {
		h.logger.Error("[handleMappingConfirmation] failed to append line to message", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}

	ok, err = b.AnswerCallbackQuery(ctx, &tgbot.AnswerCallbackQueryParams{
		CallbackQueryID: update.CallbackQuery.ID,
		Text:            confirmMessage,
		ShowAlert:       false,
	})
	if err != nil {
		h.logger.Error("[handleMappingConfirmation] failed to answer callback query", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}
	if !ok {
		h.logger.Error("[handleMappingConfirmation] failed to answer callback query: not ok")
		h.notifyInternalError(ctx, b, update)
		return
	}
}

func (h *TelegramBotHandler) handleMappingDetails(ctx context.Context, b *tgbot.Bot, update *models.Update, emailLabel enums.EmailLabel, emailID string) {
	if emailID == "" {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleMappingDetails] emailID is empty", "label", emailLabel)
		return
	}

	userID := update.CallbackQuery.From.ID

	callbackMessage := update.CallbackQuery.Message.Message
	if callbackMessage == nil {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleMappingDetails] callback message is nil", "label", emailLabel)
		return
	}

	cacheKey := fmt.Sprintf("%d:%s", userID, emailID)
	val, ok, err := h.redisClient.Get(ctx, cacheKey)
	if err != nil {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleMappingDetails] failed to get from redis", "err", err)
		return
	}
	if !ok {
		ok, err = b.AnswerCallbackQuery(ctx, &tgbot.AnswerCallbackQueryParams{
			CallbackQueryID: update.CallbackQuery.ID,
			Text:            detailsMissingMessage,
			ShowAlert:       false,
		})
		if err != nil {
			h.logger.Error("[handleMappingDetails] failed to answer callback query", "err", err)
			h.notifyInternalError(ctx, b, update)
			return
		}
		if !ok {
			h.logger.Error("[handleMappingDetails] failed to answer callback query: not ok")
			h.notifyInternalError(ctx, b, update)
			return
		}
	}

	if err := h.refreshDetailsCache(ctx, cacheKey, val); err != nil {
		h.logger.Error("[handleMappingDetails] failed to refresh details cache", "err", err, "label", emailLabel)
		h.notifyInternalError(ctx, b, update)
		return
	}

	ok, err = b.AnswerCallbackQuery(ctx, &tgbot.AnswerCallbackQueryParams{
		CallbackQueryID: update.CallbackQuery.ID,
		Text:            detailsCommingMessage,
		ShowAlert:       false,
	})
	if err != nil {
		h.logger.Error("[handleMappingDetails] failed to answer callback query", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}
	if !ok {
		h.logger.Error("[handleMappingDetails] failed to answer callback query: not ok")
		h.notifyInternalError(ctx, b, update)
		return
	}

	raw := []byte(val)
	var buf bytes.Buffer
	if err := json.Indent(&buf, raw, "", "  "); err != nil {
		h.logger.Error("[handleMappingDetails] failed to prettify json of details", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}
	pretty := buf.String()

	messageChunks := h.splitIntoChunks(pretty, maxCharsPerMessage)
	for _, chunk := range messageChunks {
		_, err := b.SendMessage(ctx, &tgbot.SendMessageParams{
			ChatID:    callbackMessage.Chat.ID,
			Text:      fmt.Sprintf("<pre>%s</pre>", chunk),
			ParseMode: models.ParseModeHTML,
		})
		if err != nil {
			h.logger.Error("[handleMappingDetails] failed to send details message in Telegram", "err", err)
			h.notifyInternalError(ctx, b, update)
			return
		}
	}
}

func (h *TelegramBotHandler) handleMappingSkip(ctx context.Context, b *tgbot.Bot, update *models.Update, emailLabel enums.EmailLabel, emailID string) {
	if emailID == "" {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleMappingSkip] emailID is empty", "label", emailLabel)
		return
	}

	callbackMessage := update.CallbackQuery.Message.Message
	if callbackMessage == nil {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleMappingSkip] callback message is nil", "label", emailLabel)
		return
	}

	_, ok, err := h.redisClient.Get(
		ctx,
		fmt.Sprintf("skipreason:%s", emailID),
	)
	if err != nil {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleMappingSkip] failed to get skip reason from redis", "err", err)
		return
	}
	if ok {
		h.logger.Info("[handleMappingSkip] skip reason already exists, skipping", "label", emailLabel)
		return
	}

	var skipButtons []models.InlineKeyboardButton
	for _, mso := range enums.GetAllMappingSkipOptions() {
		buttonText, err := h.getSkipReasonsButtonTexts(mso)
		if err != nil {
			h.logger.Error("[handleMappingSkip] failed to get skip reason button text", "err", err)
			h.notifyInternalError(ctx, b, update)
			return
		}
		skipButtons = append(skipButtons, models.InlineKeyboardButton{
			Text:         *buttonText,
			CallbackData: fmt.Sprintf("skipreason:%s:%s", mso, emailID),
		})
	}

	skipKeyboard := &models.InlineKeyboardMarkup{
		InlineKeyboard: [][]models.InlineKeyboardButton{
			skipButtons,
		},
	}

	if _, err = b.SendMessage(ctx, &tgbot.SendMessageParams{
		ChatID:      callbackMessage.Chat.ID,
		Text:        "Please select a reason for skipping the mapping:",
		ParseMode:   models.ParseModeHTML,
		ReplyMarkup: skipKeyboard,
	}); err != nil {
		h.logger.Error("[handleMappingSkip] failed to send skip reason message in Telegram", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}
}

func (h *TelegramBotHandler) handleSkipReason(ctx context.Context, b *tgbot.Bot, update *models.Update) {
	data := update.CallbackQuery.Data

	dataParts := strings.Split(data, ":")

	if len(dataParts) < 3 {
		h.logger.Error("[handleSkipReason] invalid callback data format")
		h.notifyInternalError(ctx, b, update)
		return
	}

	skipReasonStr := enums.MappingSkipOption(dataParts[1])
	emailID := dataParts[2]

	if !skipReasonStr.IsValid() {
		h.logger.Error("[handleSkipReason] invalid skip reason", "skipReason", skipReasonStr)
		h.notifyInternalError(ctx, b, update)
		return
	}

	_, err := b.AnswerCallbackQuery(ctx, &tgbot.AnswerCallbackQueryParams{
		CallbackQueryID: update.CallbackQuery.ID,
	})
	if err != nil {
		h.logger.Error("[handleSkipReason] failed to answer callback query", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}

	callbackMessage := update.CallbackQuery.Message.Message
	if callbackMessage == nil {
		h.notifyInternalError(ctx, b, update)
		h.logger.Error("[handleSkipReason] callback message is nil", "emailID", emailID)
		return
	}

	if skipReasonStr == enums.MappingSkipOptionBack {
		if err := h.removeInlineKeyboard(ctx, b, callbackMessage); err != nil {
			h.logger.Error("[handleSkipReason] failed to remove inline keyboard", "err", err)
			h.notifyInternalError(ctx, b, update)
			return
		}
		return
	}

	cacheKey := fmt.Sprintf("skipreason:%s", emailID)
	ok, err := h.redisClient.Set(ctx, cacheKey, string(skipReasonStr), skipReasonSelectedCacheTTL)
	if err != nil {
		h.logger.Error("[handleSkipReason] failed to set skip reason in redis", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}
	if !ok {
		h.logger.Error("[handleSkipReason] failed to set skip reason in redis: not ok")
		h.notifyInternalError(ctx, b, update)
		return
	}

	if err = h.removeInlineKeyboard(ctx, b, callbackMessage); err != nil {
		h.logger.Error("[handleSkipReason] failed to remove inline keyboard", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}

	replyMessage := callbackMessage.ReplyToMessage
	if err = h.appendLineToMessage(ctx, b, fmt.Sprintf("⏭️ Skipped (Reason: %s)", skipReasonStr), replyMessage); err != nil {
		h.logger.Error("[handleSkipReason] failed to append line to message", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}
	if replyMessage != nil {
		if err = h.removeInlineKeyboard(ctx, b, replyMessage); err != nil {
			h.logger.Error("[handleSkipReason] failed to remove inline keyboard from reply message", "err", err)
			h.notifyInternalError(ctx, b, update)
			return
		}
	}

	feedbackTopic := os.Getenv(KAFKA_TOPIC_FEEDBACK)

	cacheKey = fmt.Sprintf("notification:%s", emailID)
	emailMappingData, ok, err := h.redisClient.Get(ctx, cacheKey)
	if err != nil {
		h.logger.Error("[handleSkipReason] failed to get notification data from redis", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}
	if !ok {
		h.logger.Error("[handleSkipReason] notification data not found in redis", "emailID", emailID)
		h.notifyInternalError(ctx, b, update)
		return
	}

	var newMappedEmailMessageContent *dto.NewMappedEmailMessageContent
	if err = json.Unmarshal([]byte(emailMappingData), &newMappedEmailMessageContent); err != nil {
		h.logger.Error("[handleSkipReason] failed to unmarshal notification data", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}
	if newMappedEmailMessageContent == nil {
		h.logger.Error("[handleSkipReason] newMappedEmailMessageContent is nil")
		h.notifyInternalError(ctx, b, update)
		return
	}

	payload := dto.FeedbackEvent{
		EmailID:       emailID,
		ApplicationID: newMappedEmailMessageContent.MappedApplication.ID,
		Reason:        skipReasonStr,
		UserEmail:     newMappedEmailMessageContent.Email.RecipientEmail,
		CreatedAt:     time.Now(),
		Source:        "telegram_bot",
		TelegramUser: dto.TelegramUser{
			ID:       update.CallbackQuery.From.ID,
			Username: update.CallbackQuery.From.Username,
		},
		Payload: *newMappedEmailMessageContent,
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		h.logger.Error("[handleSkipReason] failed to marshal payload", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}

	if err = h.kafkaClient.Publish(
		ctx,
		feedbackTopic,
		[]byte(emailID),
		jsonPayload,
	); err != nil {
		h.logger.Error("[handleSkipReason] failed to publish skip reason to kafka", "err", err)
		h.notifyInternalError(ctx, b, update)
		return
	}
}

func (h *TelegramBotHandler) notifyInternalError(ctx context.Context, b *tgbot.Bot, update *models.Update) {
	if update.CallbackQuery == nil || update.CallbackQuery.Message.Message == nil {
		h.logger.Error("[notifyInternalError] missing callback message context")
		return
	}

	_, err := b.SendMessage(ctx, &tgbot.SendMessageParams{
		ChatID: update.CallbackQuery.Message.Message.Chat.ID,
		Text:   errorMessage,
	})
	if err != nil {
		h.logger.Error("[handleAppliedConfirmation] failed to send error message in Telegram", "err", err)
	}
}

func (h *TelegramBotHandler) removeInlineKeyboard(
	ctx context.Context,
	b *tgbot.Bot,
	message *models.Message,
) error {
	if _, err := b.EditMessageReplyMarkup(ctx, &tgbot.EditMessageReplyMarkupParams{
		ChatID:    message.Chat.ID,
		MessageID: message.ID,
	}); err != nil {
		return fmt.Errorf("failed to edit message reply markup: %w", err)
	}

	return nil
}

func (h *TelegramBotHandler) appendLineToMessage(
	ctx context.Context,
	b *tgbot.Bot,
	add string,
	message *models.Message,
) error {
	if message == nil {
		return fmt.Errorf("message is nil")
	}

	baseText := message.Text
	newText := fmt.Sprintf("%s\n\n%s", baseText, add)
	message.Text = newText

	if _, err := b.EditMessageText(ctx, &tgbot.EditMessageTextParams{
		ChatID:    message.Chat.ID,
		MessageID: message.ID,
		ParseMode: models.ParseModeHTML,
		Text:      newText,
	}); err != nil {
		return fmt.Errorf("failed to edit message text: %w", err)
	}

	return nil
}

func (h *TelegramBotHandler) splitIntoChunks(s string, chunkSize int) []string {
	var chunks []string
	runes := []rune(s)
	for i := 0; i < len(runes); i += chunkSize {
		end := i + chunkSize
		if end > len(runes) {
			end = len(runes)
		}
		chunks = append(chunks, string(runes[i:end]))
	}

	return chunks
}

func (h *TelegramBotHandler) getSkipReasonsButtonTexts(mso enums.MappingSkipOption) (*string, error) {
	if !mso.IsValid() {
		return nil, fmt.Errorf("invalid MappingSkipOption: %s", mso)
	}

	m := map[enums.MappingSkipOption]string{
		enums.MappingSkipOptionApplicationMismatch: "Application mismatch",
		enums.MappingSkipOptionEmailMisclassified:  "Email misclassified",
		enums.MappingSkipOptionOther:               "Other reason",
		enums.MappingSkipOptionBack:                "Back",
	}

	return tools.ToPtr(m[mso]), nil
}

func (h *TelegramBotHandler) refreshDetailsCache(ctx context.Context, key string, value string) error {
	ok, err := h.redisClient.Set(ctx, key, value, detailsCacheRefreshTTL)
	if err != nil {
		return fmt.Errorf("failed to refresh details cache: %w", err)
	}
	if !ok {
		return fmt.Errorf("details cache refresh returned not ok for key %s", key)
	}

	return nil
}
