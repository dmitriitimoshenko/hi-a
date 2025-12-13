package tgbt_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"mime/multipart"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/tgbt"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/tgbt/messages"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/app/tgbt/mocks"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/enums"
	"github.com/dmitriitimoshenko/hi-a/telegram-bot/internal/pkg/services/dto"
	tgbot "github.com/go-telegram/bot"
	"github.com/go-telegram/bot/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

const (
	gsaDiffUpdatePath        = "/api/application/diff/update"
	updateDBPopUpText        = "☑️ Updating internal..."
	updateSheetPopUpText     = "☑️ Updating sheet..."
	dbUpdateSuccessText      = "✅ Changes applied to the database"
	sheetUpdateSuccessText   = "✅ Google Sheet updated from the database"
	applicationDiffSkipText  = "☑️ Skipped"
	defaultCallbackQueryID   = "cbq-id"
	defaultTelegramToken     = "token"
	defaultTelegramServerURL = "http://telegram.test"
)

func TestTelegramBotHandler_HandleApplicationDiffApply(t *testing.T) {
	tests := []struct {
		name              string
		action            string
		expectedDirection enums.UpdateDirection
		expectedPopup     string
		expectedSuccess   string
	}{
		{
			name:              "apply to sheet",
			action:            "aplsh",
			expectedDirection: enums.UpdateDirectionInternal,
			expectedPopup:     updateDBPopUpText,
			expectedSuccess:   dbUpdateSuccessText,
		},
		{
			name:              "apply to internal db",
			action:            "aplin",
			expectedDirection: enums.UpdateDirectionExternal,
			expectedPopup:     updateSheetPopUpText,
			expectedSuccess:   sheetUpdateSuccessText,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			telegramClient := newTelegramMockClient(t)
			bot, err := tgbot.New(
				defaultTelegramToken,
				tgbot.WithHTTPClient(time.Second, telegramClient),
				tgbot.WithServerURL(defaultTelegramServerURL),
				tgbot.WithSkipGetMe(),
			)
			require.NoError(t, err)

			applicationSync := dto.ApplicationSync{
				EventID: "evt-apply",
				RowID:   42,
			}
			cachePayload, err := json.Marshal(applicationSync)
			require.NoError(t, err)

			cacheKey := fmt.Sprintf("%s:%s", enums.CallbackPrefixApplicationDiff, applicationSync.EventID)
			redisMock := &mocks.RedisClientMock{}
			redisMock.On("Get", mock.Anything, cacheKey).Return(string(cachePayload), true, nil).Once()
			redisMock.On("Delete", mock.Anything, cacheKey).Return(int64(1), nil).Once()

			kafkaMock := &mocks.KafkaClientMock{}

			payloadCh := make(chan messages.DiffUpdatePayload, 1)
			restoreTransport := swapHTTPTransport(t, payloadCh)
			defer restoreTransport()

			t.Setenv(tgbt.GSA_BASE_URL, "http://gsa.test")
			t.Setenv(tgbt.API_VERSION, "api-version-test")

			handler := tgbt.NewTelegramBotHandler(newTestLogger(), redisMock, kafkaMock)

			baseText := "Diff detected"
			callbackMessage := newCallbackMessage(baseText)
			update := newUpdate(
				fmt.Sprintf("application_diff:%s:%s", tt.action, applicationSync.EventID),
				callbackMessage,
			)

			handler.Handle(ctx, bot, update)

			redisMock.AssertExpectations(t)
			kafkaMock.AssertExpectations(t)

			select {
			case payload := <-payloadCh:
				require.Equal(t, applicationSync.RowID, payload.ApplicationRowID)
				require.Equal(t, tt.expectedDirection, payload.UpdateDirection)
			case <-time.After(time.Second):
				t.Fatal("expected request to GSA diff endpoint")
			}

			popupRequests := telegramClient.requestsByMethod("answerCallbackQuery")
			require.Len(t, popupRequests, 1)
			assert.Equal(t, tt.expectedPopup, popupRequests[0].fields["text"])

			removeKeyboardRequests := telegramClient.requestsByMethod("editMessageReplyMarkup")
			require.Len(t, removeKeyboardRequests, 1)
			assert.Equal(t, strconv.FormatInt(callbackMessage.Chat.ID, 10), removeKeyboardRequests[0].fields["chat_id"])
			assert.Equal(t, strconv.Itoa(callbackMessage.ID), removeKeyboardRequests[0].fields["message_id"])

			expectedMessageText := fmt.Sprintf("%s\n\n%s", baseText, tt.expectedSuccess)

			editTextRequests := telegramClient.requestsByMethod("editMessageText")
			require.Len(t, editTextRequests, 1)
			assert.Equal(t, expectedMessageText, editTextRequests[0].fields["text"])
			assert.Equal(t, expectedMessageText, callbackMessage.Text)
		})
	}
}

func TestTelegramBotHandler_HandleApplicationDiffSkip(t *testing.T) {
	ctx := context.Background()
	telegramClient := newTelegramMockClient(t)
	bot, err := tgbot.New(
		defaultTelegramToken,
		tgbot.WithHTTPClient(time.Second, telegramClient),
		tgbot.WithServerURL(defaultTelegramServerURL),
		tgbot.WithSkipGetMe(),
	)
	require.NoError(t, err)

	eventID := "evt-skip"
	cacheKey := fmt.Sprintf("%s:%s", enums.CallbackPrefixApplicationDiff, eventID)
	redisMock := &mocks.RedisClientMock{}
	redisMock.On("Delete", mock.Anything, cacheKey).Return(int64(1), nil).Once()

	kafkaMock := &mocks.KafkaClientMock{}

	handler := tgbt.NewTelegramBotHandler(newTestLogger(), redisMock, kafkaMock)

	baseText := "Diff detected"
	callbackMessage := newCallbackMessage(baseText)
	update := newUpdate(fmt.Sprintf("application_diff:skp:%s", eventID), callbackMessage)

	handler.Handle(ctx, bot, update)

	redisMock.AssertExpectations(t)
	kafkaMock.AssertExpectations(t)

	popupRequests := telegramClient.requestsByMethod("answerCallbackQuery")
	require.Len(t, popupRequests, 1)
	assert.Equal(t, applicationDiffSkipText, popupRequests[0].fields["text"])

	removeKeyboardRequests := telegramClient.requestsByMethod("editMessageReplyMarkup")
	require.Len(t, removeKeyboardRequests, 1)
	assert.Equal(t, strconv.FormatInt(callbackMessage.Chat.ID, 10), removeKeyboardRequests[0].fields["chat_id"])
	assert.Equal(t, strconv.Itoa(callbackMessage.ID), removeKeyboardRequests[0].fields["message_id"])

	expectedMessageText := fmt.Sprintf("%s\n\n%s", baseText, applicationDiffSkipText)

	editTextRequests := telegramClient.requestsByMethod("editMessageText")
	require.Len(t, editTextRequests, 1)
	assert.Equal(t, expectedMessageText, editTextRequests[0].fields["text"])
	assert.Equal(t, expectedMessageText, callbackMessage.Text)
}

func swapHTTPTransport(t *testing.T, payloadCh chan<- messages.DiffUpdatePayload) func() {
	original := http.DefaultTransport
	http.DefaultTransport = &gsaMockTransport{
		t:         t,
		payloadCh: payloadCh,
	}

	return func() {
		http.DefaultTransport = original
	}
}

type gsaMockTransport struct {
	t         *testing.T
	payloadCh chan<- messages.DiffUpdatePayload
}

func (tr *gsaMockTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	require.Equal(tr.t, http.MethodPost, req.Method)
	require.Equal(tr.t, gsaDiffUpdatePath, req.URL.Path)

	defer req.Body.Close()

	var payload messages.DiffUpdatePayload
	err := json.NewDecoder(req.Body).Decode(&payload)
	require.NoError(tr.t, err)
	require.Equal(tr.t, "api-version-test", req.Header.Get("X-API-Version"))

	select {
	case tr.payloadCh <- payload:
	default:
	}

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(`{"status":"ok"}`)),
		Request:    req,
	}

	return resp, nil
}

func newTestLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{}))
}

func newCallbackMessage(text string) *models.Message {
	return &models.Message{
		ID:   10,
		Chat: models.Chat{ID: 99, Type: "private"},
		Text: text,
	}
}

func newUpdate(callbackData string, callbackMessage *models.Message) *models.Update {
	return &models.Update{
		CallbackQuery: &models.CallbackQuery{
			ID:   defaultCallbackQueryID,
			From: models.User{ID: 777, Username: "tester"},
			Data: callbackData,
			Message: models.MaybeInaccessibleMessage{
				Type:    models.MaybeInaccessibleMessageTypeMessage,
				Message: callbackMessage,
			},
		},
	}
}

type telegramRequest struct {
	method string
	fields map[string]string
}

type telegramMockClient struct {
	t        *testing.T
	mu       sync.Mutex
	requests []telegramRequest
}

func newTelegramMockClient(t *testing.T) *telegramMockClient {
	return &telegramMockClient{t: t}
}

func (c *telegramMockClient) Do(req *http.Request) (*http.Response, error) {
	method := path.Base(req.URL.Path)
	fields := c.parseFields(req)

	c.mu.Lock()
	c.requests = append(c.requests, telegramRequest{
		method: method,
		fields: fields,
	})
	c.mu.Unlock()

	var result string

	switch method {
	case "answerCallbackQuery":
		result = "true"
	case "editMessageReplyMarkup", "editMessageText", "sendMessage":
		chatID := fields["chat_id"]
		messageID := fields["message_id"]
		if messageID == "" {
			messageID = "0"
		}
		result = fmt.Sprintf(`{"message_id":%s,"chat":{"id":%s,"type":"private"},"date":0}`, messageID, chatID)
	default:
		c.t.Fatalf("unexpected telegram method %s", method)
	}

	resp := &http.Response{
		StatusCode: http.StatusOK,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(fmt.Sprintf(`{"ok":true,"result":%s}`, result))),
	}

	return resp, nil
}

func (c *telegramMockClient) requestsByMethod(method string) []telegramRequest {
	c.mu.Lock()
	defer c.mu.Unlock()

	var filtered []telegramRequest
	for _, req := range c.requests {
		if req.method == method {
			filtered = append(filtered, req)
		}
	}

	return filtered
}

func (c *telegramMockClient) parseFields(req *http.Request) map[string]string {
	contentType := req.Header.Get("Content-Type")
	mediaType, params, err := mime.ParseMediaType(contentType)
	require.NoError(c.t, err)
	require.Equal(c.t, "multipart/form-data", mediaType)

	reader := multipart.NewReader(req.Body, params["boundary"])

	fields := make(map[string]string)
	for {
		part, err := reader.NextPart()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(c.t, err)

		data, err := io.ReadAll(part)
		require.NoError(c.t, err)
		fields[part.FormName()] = string(data)
	}

	return fields
}
