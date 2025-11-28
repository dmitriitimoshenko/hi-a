from __future__ import annotations

from dataclasses import dataclass
from typing import Tuple

import numpy as np


@dataclass
class Assignment:
    row_indices: np.ndarray
    column_indices: np.ndarray


class HungarianSolver:
    def __init__(self, cost_matrix: np.ndarray) -> None:
        if cost_matrix.ndim != 2:
            message = "HungarianSolver requires a 2D matrix"
            raise ValueError(message)

        rows, cols = cost_matrix.shape
        if rows != cols:
            message = "HungarianSolver requires a square matrix"
            raise ValueError(message)

        self._cost = cost_matrix.astype(float).copy()
        self._size = rows
        self._mask = np.zeros_like(self._cost, dtype=int)
        self._row_covered = np.zeros(self._size, dtype=bool)
        self._col_covered = np.zeros(self._size, dtype=bool)
        self._zero_rc: tuple[int, int] = (-1, -1)
        self._epsilon = 1e-5

    def solve(self) -> Assignment:
        step = 1

        while True:
            if step == 1:
                self._step1()
                step = 2
            elif step == 2:
                step = self._step2()
            elif step == 3:
                step = self._step3()
            elif step == 4:
                step = self._step4()
            elif step == 5:
                step = self._step5()
            elif step == 6:
                step = self._step6()
            elif step == 7:
                return self._extract_assignment()
            else:
                message = f"Unsupported step for HungarianSolver: {step}"
                raise ValueError(message)

    def _step1(self) -> None:
        row_minima = self._cost.min(axis=1)
        self._cost -= row_minima[:, np.newaxis]

    def _step2(self) -> int:
        for row in range(self._size):
            for col in range(self._size):
                value = self._cost[row, col]
                if not self._is_zero(value):
                    continue

                if self._mask[row, col] == 0 and not self._row_has_star(row) and not self._col_has_star(col):
                    self._mask[row, col] = 1

        return 3

    def _step3(self) -> int:
        self._col_covered[:] = False

        for col in range(self._size):
            if np.any(self._mask[:, col] == 1):
                self._col_covered[col] = True

        covered_columns = int(self._col_covered.sum())

        if covered_columns >= self._size:
            return 7

        return 4

    def _step4(self) -> int:
        while True:
            row, col = self._find_zero()
            if row is None or col is None:
                return 6

            self._mask[row, col] = 2
            star_column = self._find_star_in_row(row)

            if star_column is None:
                self._zero_rc = (row, col)
                return 5

            self._row_covered[row] = True
            self._col_covered[star_column] = False

    def _step5(self) -> int:
        path: list[tuple[int, int]] = [self._zero_rc]
        done = False

        while not done:
            last_row, last_col = path[-1]
            star_row = self._find_star_in_column(last_col)

            if star_row is None:
                done = True
                continue

            path.append((star_row, last_col))
            prime_col = self._find_prime_in_row(star_row)

            if prime_col is None:
                message = "Prime zero not found when expected"
                raise ValueError(message)

            path.append((star_row, prime_col))

        for row, col in path:
            if self._mask[row, col] == 1:
                self._mask[row, col] = 0
            else:
                self._mask[row, col] = 1

        self._row_covered[:] = False
        self._col_covered[:] = False
        self._erase_primes()

        return 3

    def _step6(self) -> int:
        min_value = self._find_smallest_uncovered()

        for row in range(self._size):
            if self._row_covered[row]:
                self._cost[row, :] += min_value

        for col in range(self._size):
            if not self._col_covered[col]:
                self._cost[:, col] -= min_value

        return 4

    def _extract_assignment(self) -> Assignment:
        rows, cols = np.where(self._mask == 1)
        return Assignment(row_indices=rows, column_indices=cols)

    def _find_zero(self) -> tuple[int | None, int | None]:
        for row in range(self._size):
            if self._row_covered[row]:
                continue

            for col in range(self._size):
                if self._col_covered[col]:
                    continue

                value = self._cost[row, col]
                if self._is_zero(value):
                    return row, col

        return None, None

    def _row_has_star(self, row: int) -> bool:
        return bool(np.any(self._mask[row, :] == 1))

    def _col_has_star(self, col: int) -> bool:
        return bool(np.any(self._mask[:, col] == 1))

    def _find_star_in_row(self, row: int) -> int | None:
        starred_columns = np.where(self._mask[row, :] == 1)[0]
        if starred_columns.size == 0:
            return None

        return int(starred_columns[0])

    def _find_star_in_column(self, col: int) -> int | None:
        starred_rows = np.where(self._mask[:, col] == 1)[0]
        if starred_rows.size == 0:
            return None

        return int(starred_rows[0])

    def _find_prime_in_row(self, row: int) -> int | None:
        prime_columns = np.where(self._mask[row, :] == 2)[0]
        if prime_columns.size == 0:
            return None

        return int(prime_columns[0])

    def _erase_primes(self) -> None:
        self._mask[self._mask == 2] = 0

    def _find_smallest_uncovered(self) -> float:
        uncovered = []

        for row in range(self._size):
            if self._row_covered[row]:
                continue

            for col in range(self._size):
                if self._col_covered[col]:
                    continue

                uncovered.append(self._cost[row, col])

        if not uncovered:
            return 0.0

        return float(min(uncovered))

    def _is_zero(self, value: float) -> bool:
        return abs(value) <= self._epsilon


def linear_sum_assignment(cost_matrix: np.ndarray) -> Tuple[np.ndarray, np.ndarray]:
    solver = HungarianSolver(cost_matrix)
    assignment = solver.solve()

    return assignment.row_indices, assignment.column_indices
