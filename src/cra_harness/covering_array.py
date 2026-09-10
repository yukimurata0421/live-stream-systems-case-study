"""Deterministic mixed-level covering arrays for bounded CRA campaigns."""

from __future__ import annotations

import random
from itertools import combinations, product


def _covered(row: tuple[int, ...], strength: int) -> set[tuple[tuple[int, ...], tuple[int, ...]]]:
    return {(indices, tuple(row[index] for index in indices)) for indices in combinations(range(len(row)), strength)}


def generate_covering_array(
    factor_sizes: tuple[int, ...],
    *,
    strength: int,
    target_count: int,
    mandatory_rows: tuple[tuple[int, ...], ...] = (),
    seed: int,
) -> tuple[tuple[int, ...], ...]:
    """Generate an IPOG-style array, then add mandatory and unique padding rows."""

    if isinstance(strength, bool) or not isinstance(strength, int) or not 2 <= strength <= len(factor_sizes):
        raise ValueError("COVERING_ARRAY_STRENGTH_INVALID")
    if any(isinstance(size, bool) or not isinstance(size, int) or not 2 <= size <= 64 for size in factor_sizes):
        raise ValueError("COVERING_ARRAY_FACTOR_SIZE_INVALID")
    if isinstance(target_count, bool) or not isinstance(target_count, int) or target_count < 1:
        raise ValueError("COVERING_ARRAY_TARGET_COUNT_INVALID")
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ValueError("COVERING_ARRAY_SEED_INVALID")
    ordered = sorted(range(len(factor_sizes)), key=lambda index: (-factor_sizes[index], index))
    inverse = {original: position for position, original in enumerate(ordered)}
    sizes = tuple(factor_sizes[index] for index in ordered)
    rows = [list(values) for values in product(*(range(size) for size in sizes[:strength]))]

    for new_index in range(strength, len(sizes)):
        uncovered: set[tuple[tuple[int, ...], tuple[int, ...]]] = set()
        for previous in combinations(range(new_index), strength - 1):
            indices = (*previous, new_index)
            for values in product(*(range(sizes[index]) for index in indices)):
                uncovered.add((indices, values))

        for row in rows:
            candidate = max(
                range(sizes[new_index]),
                key=lambda value: sum(
                    (((*previous, new_index), tuple(row[index] for index in previous) + (value,)) in uncovered)
                    for previous in combinations(range(new_index), strength - 1)
                ),
            )
            row.append(candidate)
            for previous in combinations(range(new_index), strength - 1):
                uncovered.discard(((*previous, new_index), tuple(row[index] for index in previous) + (candidate,)))

        while uncovered:
            indices, values = next(iter(uncovered))
            assigned = dict(zip(indices, values, strict=True))
            new_value = assigned[new_index]
            while len(assigned) < new_index + 1:
                previous_assigned = [index for index in assigned if index != new_index]
                choices: list[tuple[int, int, int, int, int]] = []
                for index in range(new_index):
                    if index in assigned:
                        continue
                    for value in range(sizes[index]):
                        score = 0
                        for existing in combinations(previous_assigned, strength - 2):
                            previous = tuple(sorted((*existing, index)))
                            candidate_values = tuple(value if item == index else assigned[item] for item in previous) + (new_value,)
                            score += ((*previous, new_index), candidate_values) in uncovered
                        choices.append((score, -index, -value, index, value))
                _, _, _, selected_index, selected_value = max(choices)
                assigned[selected_index] = selected_value
            new_row = [assigned[index] for index in range(new_index + 1)]
            rows.append(new_row)
            for previous in combinations(range(new_index), strength - 1):
                uncovered.discard(((*previous, new_index), tuple(new_row[index] for index in previous) + (new_value,)))

    restored = [tuple(row[inverse[index]] for index in range(len(factor_sizes))) for row in rows]
    unique = set(restored)
    if len(unique) != len(restored):
        raise AssertionError("COVERING_ARRAY_GENERATOR_DUPLICATE")
    for mandatory in mandatory_rows:
        if len(mandatory) != len(factor_sizes) or any(not 0 <= value < factor_sizes[index] for index, value in enumerate(mandatory)):
            raise ValueError("COVERING_ARRAY_MANDATORY_ROW_INVALID")
        if mandatory not in unique:
            restored.append(mandatory)
            unique.add(mandatory)
    if len(restored) > target_count:
        raise ValueError(f"COVERING_ARRAY_TARGET_TOO_LOW:{len(restored)}")
    rng = random.Random(seed)
    while len(restored) < target_count:
        padded = tuple(rng.randrange(size) for size in factor_sizes)
        if padded not in unique:
            restored.append(padded)
            unique.add(padded)
    return tuple(restored)


def coverage_report(rows: tuple[tuple[int, ...], ...], factor_sizes: tuple[int, ...], *, strength: int) -> dict[str, int | bool]:
    observed: set[tuple[tuple[int, ...], tuple[int, ...]]] = set()
    for row in rows:
        observed.update(_covered(row, strength))
    expected = sum(
        __import__("math").prod(factor_sizes[index] for index in indices) for indices in combinations(range(len(factor_sizes)), strength)
    )
    return {
        "strength": strength,
        "expected_combination_count": expected,
        "observed_combination_count": len(observed),
        "missing_combination_count": expected - len(observed),
        "coverage_complete": len(observed) == expected,
    }
