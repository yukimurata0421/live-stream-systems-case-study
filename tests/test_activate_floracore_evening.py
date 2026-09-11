from __future__ import annotations

import os
import tempfile
import unittest
from pathlib import Path

from ops.scripts.activate_floracore_evening import EXPECTED_TRACKS, NCS_ARCHIVE_RELATIVE, activate


class ActivateFloracoreEveningTests(unittest.TestCase):
    def make_library(self, root: Path) -> Path:
        music_root = root / "ncs_music"
        tracks = music_root / "floracore_evening" / "tracks"
        tracks.mkdir(parents=True)
        for index in range(1, EXPECTED_TRACKS + 1):
            (tracks / f"Floracore - Track {index:02d}.mp3").touch()
        return music_root

    def test_dry_run_does_not_create_links(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_dir:
            music_root = self.make_library(Path(temporary_dir))
            result = activate(music_root, apply=False, credit_confirmed=False)

            self.assertEqual(result["planned"], EXPECTED_TRACKS)
            self.assertEqual(result["created"], 0)
            self.assertFalse((music_root / "time_tags" / "evening").exists())

    def test_apply_requires_credit_confirmation(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_dir:
            music_root = self.make_library(Path(temporary_dir))

            with self.assertRaisesRegex(RuntimeError, "--credit-confirmed"):
                activate(music_root, apply=True, credit_confirmed=False)

    def test_apply_creates_idempotent_relative_evening_links(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_dir:
            music_root = self.make_library(Path(temporary_dir))
            first = activate(music_root, apply=True, credit_confirmed=True)
            second = activate(music_root, apply=True, credit_confirmed=True)

            target = music_root / "time_tags" / "evening" / "minor_Floracore - Track 01.mp3"
            self.assertEqual(first["created"], EXPECTED_TRACKS)
            self.assertEqual(second["existing"], EXPECTED_TRACKS)
            self.assertTrue(target.is_symlink())
            self.assertEqual(
                os.readlink(target),
                "../../floracore_evening/tracks/Floracore - Track 01.mp3",
            )
            self.assertTrue(target.resolve().is_file())

    def test_apply_archives_ncs_links_and_leaves_evening_exclusive(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_dir:
            music_root = self.make_library(Path(temporary_dir))
            ncs_source = music_root / "minor" / "NCS Track.mp3"
            ncs_source.parent.mkdir(parents=True)
            ncs_source.touch()
            evening = music_root / "time_tags" / "evening"
            evening.mkdir(parents=True)
            ncs_link = evening / "minor_NCS Track.mp3"
            os.symlink("../../minor/NCS Track.mp3", ncs_link)

            dry_run = activate(music_root, apply=False, credit_confirmed=False)
            self.assertEqual(dry_run["ncs_to_archive"], 1)
            self.assertEqual(dry_run["ncs_archived"], 0)
            self.assertTrue(ncs_link.is_symlink())

            applied = activate(music_root, apply=True, credit_confirmed=True)
            archive = music_root / NCS_ARCHIVE_RELATIVE / ncs_link.name
            self.assertEqual(applied["ncs_archived"], 1)
            self.assertFalse(ncs_link.exists())
            self.assertTrue(archive.is_symlink())
            self.assertEqual(os.readlink(archive), "../../minor/NCS Track.mp3")
            self.assertEqual(
                len(list(evening.glob("minor_Floracore - *.mp3"))),
                EXPECTED_TRACKS,
            )
            self.assertEqual(len(list(evening.iterdir())), EXPECTED_TRACKS)

            repeated = activate(music_root, apply=True, credit_confirmed=True)
            self.assertEqual(repeated["ncs_to_archive"], 0)
            self.assertEqual(repeated["ncs_archived"], 0)

    def test_apply_refuses_to_archive_non_symlink_evening_entry(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_dir:
            music_root = self.make_library(Path(temporary_dir))
            evening = music_root / "time_tags" / "evening"
            evening.mkdir(parents=True)
            (evening / "minor_unexpected.mp3").touch()

            with self.assertRaisesRegex(RuntimeError, "non-symlink"):
                activate(music_root, apply=True, credit_confirmed=True)


if __name__ == "__main__":
    unittest.main()
