from typing import Optional

import gcsfs


def find_manifest(
    folder_name: str,
    k_suffix: str,
    manifest_prefixes: dict[str, str],
) -> Optional[dict[str, str]]:
    """
    Verilen GCS folder içerisinde ilgili K için manifest'i bulur.

    Args:
        folder_name:
            GCS folder path.

        k_suffix:
            Manifest mapping içerisindeki key.
            Örn: "k1", "k2", "k3"

        manifest_prefixes:
            K -> manifest prefix mapping'i.
            Örn:
                {
                    "k1": "eskimi",
                    "k2": "location",
                    "k3": "irys",
                    "k4": "veraset",
                }

    Returns:
        Manifest bulunduysa:

        {
            "folder_name": "...",
            "file_name": "...",
            "manifest_path": "gs://..."
        }

        Bulunamazsa None.
    """

    if k_suffix not in manifest_prefixes:
        raise ValueError(
            f"Unknown k_suffix: {k_suffix}. "
            f"Expected one of: {list(manifest_prefixes.keys())}"
        )

    expected_prefix = manifest_prefixes[k_suffix]

    folder_name = folder_name.rstrip("/") + "/"

    fs = gcsfs.GCSFileSystem()

    files = fs.ls(folder_name)

    for file_path in files:

        file_name = file_path.rstrip("/").split("/")[-1]

        # Manifest değilse geç
        if not file_name.endswith("manifest.txt"):
            continue

        # "_" veya "-" ile prefix'i ayır
        separators = ["_", "-"]

        manifest_prefix = None

        for separator in separators:

            if separator in file_name:
                manifest_prefix = file_name.split(
                    separator,
                    1,
                )[0]
                break

        if manifest_prefix != expected_prefix:
            continue

        manifest_path = (
            file_path
            if file_path.startswith("gs://")
            else f"gs://{file_path}"
        )

        return {
            "folder_name": folder_name,
            "file_name": manifest_prefix,
            "manifest_path": manifest_path,
        }

    return None