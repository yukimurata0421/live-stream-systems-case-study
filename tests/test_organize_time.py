from __future__ import annotations

import sys
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src" / "dj"))

import music_time_classifier  # type: ignore
from dj import organize_time as organize_time_cli
from music_classifier import classification as classifier_classification  # type: ignore
from music_classifier import duration as classifier_duration  # type: ignore


def test_filename_genre_wins_over_metadata_description_noise() -> None:
    path = Path("BEAUZ & JVNA - Crazy ｜ Electronic Pop.mp3")

    with mock.patch.object(classifier_classification, "read_audio") as read_audio:
        slot, genre, source = music_time_classifier.classify_track(path)

    assert slot == "day"
    assert genre == "Electronic Pop"
    assert source == "filename"
    read_audio.assert_not_called()


def test_organize_time_cli_defaults_to_stream_v3_paths() -> None:
    assert organize_time_cli.PROJECT_ROOT == ROOT
    assert organize_time_cli.DEFAULT_MUSIC_ROOT == ROOT / "ncs_music"
    assert organize_time_cli.DEFAULT_OVERRIDES_FILE == ROOT / "configs" / "auto_dj_classification_overrides.json"


def test_title_tag_is_used_when_filename_has_no_genre() -> None:
    audio = {
        "TIT2": (
            "Cartoon, Jéja - C U Again ft. Mikk Mäe "
            "(Cartoon, Jéja, Futuristik VIP) | Electronic | NCS"
        )
    }

    with mock.patch.object(classifier_classification, "read_audio", return_value=audio):
        slot, genre, source = music_time_classifier.classify_track(
            Path("Cartoon, Jéja - C U Again ft. Mikk Mäe.mp3")
        )

    assert slot == "night"
    assert genre == "Electronic"
    assert source == "title"


def test_filename_genre_uses_first_non_ncs_pipe_segment() -> None:
    path = Path(
        "nuphory, Chikaya - Make Me Feel ｜ Rally House ｜ "
        "NCS x Aurorian Records - Copyright Free Music.mp3"
    )

    slot, genre, source = music_time_classifier.classify_track(path)

    assert slot == "morning"
    assert genre == "Rally House"
    assert source == "filename"


def test_exclude_pattern_does_not_match_inside_remix() -> None:
    assert music_time_classifier.normalized_phrase_in_text("mix", "NCS Album Mix")
    assert not music_time_classifier.normalized_phrase_in_text("mix", "Andromedik Remix")


def test_track_title_normalization_keeps_ft_suffix_without_mp3_extension() -> None:
    assert (
        music_time_classifier.normalize_track_title("D'Amico & Valax - Mirage Of My Heart (ft. Fayzone)")
        == music_time_classifier.normalize_track_title("D'Amico & Valax - Mirage Of My Heart (ft. Fayzone).mp3")
    )
    assert (
        music_time_classifier.normalize_track_title("Donkong - Binary (feat. HANNY)")
        == music_time_classifier.normalize_track_title("Donkong - Binary (feat. HANNY).mp3")
    )


def test_specific_house_genres_do_not_collapse_to_house() -> None:
    assert music_time_classifier.classify_genre("Future House") == "day"
    assert music_time_classifier.classify_genre("Bass House") == "evening"
    assert music_time_classifier.classify_genre("House") == "day"


def test_electronic_pop_does_not_match_plain_electronic() -> None:
    assert music_time_classifier.classify_genre("Electronic Pop") == "day"
    assert music_time_classifier.classify_genre("Electronic") == "night"


def test_user_time_policy_examples() -> None:
    assert music_time_classifier.classify_genre("Progressive House") == "morning"
    assert music_time_classifier.classify_genre("Tropical House") == "morning"
    assert music_time_classifier.classify_genre("Electro House") == "day"
    assert music_time_classifier.classify_genre("DnB") == "day"
    assert music_time_classifier.classify_genre("Future Bass") == "evening"
    assert music_time_classifier.classify_genre("Trap") == "evening"
    assert music_time_classifier.classify_genre("Dubstep") == "evening"
    assert music_time_classifier.classify_genre("Deep House") == "night"
    assert music_time_classifier.classify_genre("Liquid DnB") == "night"
    assert music_time_classifier.classify_genre("Phonk") == "night"


def test_long_tracks_are_skipped_when_organizing(tmp_path: Path) -> None:
    base = tmp_path / "music"
    target = base / "time_tags"
    source = base / "major"
    source.mkdir(parents=True)
    long_track = source / "EDM - 1 Hour MIX ｜ Electronic.mp3"
    long_track.write_bytes(b"")

    with mock.patch.object(classifier_duration, "track_duration_sec", return_value=3600.0):
        counts = music_time_classifier.organize_library(base, target)

    assert counts["major"]["skipped_long"] == 1
    assert not list((target / "night").glob("*.mp3"))


def test_duration_uses_ffprobe_when_mutagen_is_unavailable(tmp_path: Path) -> None:
    path = tmp_path / "track.mp3"
    path.write_bytes(b"")

    with mock.patch.object(classifier_duration, "read_audio", return_value=None):
        with mock.patch.object(classifier_duration.shutil, "which", return_value="/usr/bin/ffprobe"):
            with mock.patch.object(classifier_duration.subprocess, "run") as run:
                run.return_value.returncode = 0
                run.return_value.stdout = "612.5\n"
                assert music_time_classifier.track_duration_sec(path) == 612.5


def test_ncs_album_asset_uses_duration_probe() -> None:
    assert music_time_classifier.should_probe_duration(Path("NCS ： Alpha.mp3"))


def test_duplicate_titles_keep_single_best_classified_track(tmp_path: Path) -> None:
    base = tmp_path / "music"
    target = base / "time_tags"
    major = base / "major"
    minor = base / "minor"
    major.mkdir(parents=True)
    minor.mkdir(parents=True)
    best = major / "Cartoon, Jéja - On & On (feat. Daniel Levi) ｜ Electronic Pop.mp3"
    duplicate = minor / "Cartoon, Jéja - On & On (feat. Daniel Levi).mp3"
    best.write_bytes(b"")
    duplicate.write_bytes(b"")

    with mock.patch.object(classifier_duration, "track_duration_sec", return_value=180.0):
        counts = music_time_classifier.organize_library(base, target)

    assert counts["major"]["day"] == 1
    assert counts["minor"]["skipped_duplicate"] == 1
    assert (target / "day" / f"major_{best.name}").is_symlink()
    assert not (target / "others" / f"minor_{duplicate.name}").exists()


def test_unknown_short_track_uses_artist_slot_inference(tmp_path: Path) -> None:
    base = tmp_path / "music"
    target = base / "time_tags"
    minor = base / "minor"
    minor.mkdir(parents=True)
    known = minor / "Example Artist - Known ｜ Trap.mp3"
    unknown = minor / "Example Artist - Unknown.mp3"
    known.write_bytes(b"")
    unknown.write_bytes(b"")

    with mock.patch.object(classifier_duration, "track_duration_sec", return_value=180.0):
        counts = music_time_classifier.organize_library(base, target)

    assert counts["minor"]["evening"] == 2
    assert (target / "evening" / f"minor_{unknown.name}").is_symlink()


def test_confirmed_track_override_classifies_unknown_track(tmp_path: Path) -> None:
    base = tmp_path / "music"
    target = base / "time_tags"
    minor = base / "minor"
    minor.mkdir(parents=True)
    unknown = minor / "Hyri - Nocturnal.mp3"
    unknown.write_bytes(b"")
    overrides = {
        "track_overrides": {
            "Hyri - Nocturnal": {
                "slot": "night",
                "confidence": "confirmed",
                "reason": "manual listening check",
            }
        }
    }

    with mock.patch.object(classifier_duration, "track_duration_sec", return_value=180.0):
        counts = music_time_classifier.organize_library(base, target, overrides=overrides)

    assert counts["minor"]["night"] == 1
    assert (target / "night" / f"minor_{unknown.name}").is_symlink()
    assert not (target / "others" / f"minor_{unknown.name}").exists()


def test_candidate_track_override_stays_report_only(tmp_path: Path) -> None:
    base = tmp_path / "music"
    target = base / "time_tags"
    minor = base / "minor"
    minor.mkdir(parents=True)
    unknown = minor / "Hyri - Nocturnal.mp3"
    unknown.write_bytes(b"")
    overrides = {
        "track_overrides": {
            "Hyri - Nocturnal": {
                "slot": "night",
                "confidence": "candidate",
                "reason": "not listened yet",
            }
        }
    }

    with mock.patch.object(classifier_duration, "track_duration_sec", return_value=180.0):
        counts = music_time_classifier.organize_library(base, target, overrides=overrides)
        report = music_time_classifier.build_others_report(base, overrides=overrides)

    assert counts["minor"]["others"] == 1
    assert (target / "others" / f"minor_{unknown.name}").is_symlink()
    assert report["items"][0]["candidate_action"] == "classify_pending_confirmation"
    assert report["items"][0]["candidate_slot"] == "night"


def test_candidate_artist_override_stays_report_only(tmp_path: Path) -> None:
    base = tmp_path / "music"
    target = base / "time_tags"
    minor = base / "minor"
    minor.mkdir(parents=True)
    unknown = minor / "JOXION - Go Off.mp3"
    unknown.write_bytes(b"")
    overrides = {
        "artist_overrides": {
            "JOXION": {
                "slot": "evening",
                "confidence": "candidate",
                "reason": "not listened yet",
            }
        }
    }

    with mock.patch.object(classifier_duration, "track_duration_sec", return_value=180.0):
        counts = music_time_classifier.organize_library(base, target, overrides=overrides)
        report = music_time_classifier.build_others_report(base, overrides=overrides)

    assert counts["minor"]["others"] == 1
    assert (target / "others" / f"minor_{unknown.name}").is_symlink()
    assert report["items"][0]["candidate_action"] == "classify_pending_confirmation"
    assert report["items"][0]["candidate_slot"] == "evening"


def test_confirmed_artist_override_classifies_unknown_artist_track(tmp_path: Path) -> None:
    base = tmp_path / "music"
    target = base / "time_tags"
    minor = base / "minor"
    minor.mkdir(parents=True)
    unknown = minor / "JOXION - Go Off.mp3"
    unknown.write_bytes(b"")
    overrides = {
        "artist_overrides": {
            "JOXION": {
                "slot": "evening",
                "confidence": "confirmed",
                "reason": "listened",
            }
        }
    }

    with mock.patch.object(classifier_duration, "track_duration_sec", return_value=180.0):
        counts = music_time_classifier.organize_library(base, target, overrides=overrides)

    assert counts["minor"]["evening"] == 1
    assert (target / "evening" / f"minor_{unknown.name}").is_symlink()
    assert not (target / "others" / f"minor_{unknown.name}").exists()


def test_confirmed_exclude_pattern_skips_non_rotation_asset(tmp_path: Path) -> None:
    base = tmp_path / "music"
    target = base / "time_tags"
    minor = base / "minor"
    minor.mkdir(parents=True)
    asset = minor / "NCS Reloaded： Behind The Scenes.mp3"
    asset.write_bytes(b"")
    overrides = {
        "exclude_patterns": [
            {
                "pattern": "behind the scenes",
                "confidence": "confirmed",
                "reason": "program asset",
            }
        ]
    }

    with mock.patch.object(classifier_duration, "track_duration_sec", return_value=180.0):
        counts = music_time_classifier.organize_library(base, target, overrides=overrides)

    assert counts["minor"]["skipped_excluded"] == 1
    assert not list((target / "others").glob("*.mp3"))


def test_others_report_marks_non_rotation_and_artist_cluster(tmp_path: Path) -> None:
    base = tmp_path / "music"
    minor = base / "minor"
    minor.mkdir(parents=True)
    asset = minor / "NCS Mashup - Biggest NoCopyrightSounds Songs.mp3"
    first = minor / "JOXION - Go Off.mp3"
    second = minor / "JOXION - Next Level.mp3"
    asset.write_bytes(b"")
    first.write_bytes(b"")
    second.write_bytes(b"")

    with mock.patch.object(classifier_duration, "track_duration_sec", return_value=180.0):
        report = music_time_classifier.build_others_report(base)

    by_title = {item["title"]: item for item in report["items"]}
    assert by_title["NCS Mashup - Biggest NoCopyrightSounds Songs"]["candidate_action"] == "exclude_candidate"
    assert by_title["JOXION - Go Off"]["candidate_action"] == "review_artist_cluster"
    assert by_title["JOXION - Next Level"]["candidate_action"] == "review_artist_cluster"
