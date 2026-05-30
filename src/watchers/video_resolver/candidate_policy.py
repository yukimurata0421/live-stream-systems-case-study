from __future__ import annotations


def choose_video_candidate(
    candidates: list[tuple[str, str]],
    *,
    expected_video_id: str,
    url_preservation_active: bool,
) -> tuple[str, str, dict]:
    clean_candidates: list[tuple[str, str]] = []
    seen: set[str] = set()
    for video_id, source in candidates:
        vid = str(video_id or "").strip()
        if not vid or vid in seen:
            continue
        seen.add(vid)
        clean_candidates.append((vid, str(source or "unknown").strip() or "unknown"))

    expected = str(expected_video_id or "").strip()
    details = {
        "expected_video_id": expected,
        "candidate_new_url_found": False,
        "candidate_new_video_id": "",
        "candidate_new_video_source": "",
        "candidate_new_video_reason": "",
        "selected_candidate_policy": "first_available",
    }
    if url_preservation_active and expected:
        for video_id, source in clean_candidates:
            if video_id == expected:
                details["selected_candidate_policy"] = "preserve_expected_url"
                return video_id, source, details
        for video_id, source in clean_candidates:
            if video_id != expected:
                details.update(
                    {
                        "candidate_new_url_found": True,
                        "candidate_new_video_id": video_id,
                        "candidate_new_video_source": source,
                        "candidate_new_video_reason": (
                            "different video id observed during url preservation window; kept expected URL"
                        ),
                        "selected_candidate_policy": "preserve_expected_url_candidate_held",
                    }
                )
                return expected, "expected_url_preserved", details

    for video_id, source in clean_candidates:
        if expected and video_id != expected:
            details.update(
                {
                    "candidate_new_url_found": True,
                    "candidate_new_video_id": video_id,
                    "candidate_new_video_source": source,
                    "candidate_new_video_reason": "different video id selected after url preservation window",
                    "selected_candidate_policy": "new_url_allowed_after_window",
                }
            )
        return video_id, source, details
    return "", "none", details
