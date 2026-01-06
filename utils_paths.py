from pathlib import Path
import os
import shutil
import time


def get_dropbox_root() -> Path | None:
    home = Path.home()
    dropbox = home / "Dropbox"
    return dropbox if dropbox.exists() else None


def get_onedrive_root() -> Path | None:
    if "OneDriveCommercial" in os.environ:
        return Path(os.environ["OneDriveCommercial"])
    if "OneDrive" in os.environ:
        return Path(os.environ["OneDrive"])
    return None


def force_copy_planning_to_onedrive() -> Path:
    dropbox = get_dropbox_root()
    onedrive = get_onedrive_root()

    if not dropbox:
        raise RuntimeError("❌ Dropbox introuvable sur ce PC")
    if not onedrive:
        raise RuntimeError("❌ OneDrive introuvable sur ce PC")

    src = dropbox / "Goldenlines" / "Planning 2026.xlsx"
    dst = onedrive / "Planning 2026.xlsx"
    tmp = dst.with_suffix(".tmp")

    if not src.exists():
        raise FileNotFoundError(f"❌ Fichier source absent : {src}")

    shutil.copy2(src, tmp)

    for _ in range(5):
        try:
            if dst.exists():
                os.remove(dst)
            break
        except PermissionError:
            time.sleep(1)

    os.replace(tmp, dst)
    return dst
