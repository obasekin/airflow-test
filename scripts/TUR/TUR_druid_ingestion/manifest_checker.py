from typing import Optional, Dict
import gcsfs


# K -> manifest dosyasının prefix'i
K_MANIFEST_PREFIX = {
    "k1": "eskimi",
    "k2": "location",
    "k3": "iris",
    "k4": "veraset",
}


def find_manifest(folder_name: str, k_suffix: str) -> Optional[Dict[str, str]]:
    """
    Verilen GCS folder içerisinde ilgili K için manifest'i bulur.

    Dosya isimlerinde hem '_' hem '-' ayraçları desteklenir.

    Örnek:
        location_batch_manifest.txt
        location-batch-manifest.txt

    K3 için geçici olarak:
        1_part_manifest.txt
        0_part_manifest.txt

    dosyaları da desteklenir.

    Return:
        {
            "folder_name": "...",
            "file_name": "...",
            "manifest_path": "gs://..."
        }

        Bulamazsa None.
    """

    if k_suffix not in K_MANIFEST_PREFIX:
        raise ValueError(
            f"Unknown k_suffix: {k_suffix}. "
            f"Expected one of: {list(K_MANIFEST_PREFIX.keys())}"
        )

    expected_prefix = K_MANIFEST_PREFIX[k_suffix]

    # Folder sonunda / yoksa ekle
    folder_name = folder_name.rstrip("/") + "/"

    fs = gcsfs.GCSFileSystem()

    # Folder içerisindeki dosyaları listele
    files = fs.ls(folder_name)

    for file_path in files:
        file_name = file_path.rstrip("/").split("/")[-1]

        # Manifest değilse geç
        if not file_name.endswith("manifest.txt"):
            continue

        # ------------------------------------------------------------
        # K3 - GEÇİCİ FORMAT
        # ------------------------------------------------------------
        #
        # Şimdilik:
        #   1_part_manifest.txt
        #   0_part_manifest.txt
        #
        # geliyor.
        #
        # K3 = iris olsa bile bunları kabul ediyoruz.
        #
        if k_suffix == "k3":
            temporary_prefixes = {"1_part", "0_part"}

            # Hem "_" hem "-" destekle
            manifest_prefix = (
                file_name.split("_", 1)[0]
                if "_" in file_name
                else file_name.split("-", 1)[0]
            )

            if (
                manifest_prefix in temporary_prefixes
                or file_name.startswith(expected_prefix + "_")
                or file_name.startswith(expected_prefix + "-")
            ):
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

            continue

        # ------------------------------------------------------------
        # NORMAL FORMAT
        # ------------------------------------------------------------
        #
        # Örnek:
        #   location_batch_manifest.txt
        #   location-batch-manifest.txt
        #
        # Prefix'i hem "_" hem "-" ile ayır.
        #
        separators = ["_", "-"]

        manifest_prefix = None

        for separator in separators:
            if separator in file_name:
                manifest_prefix = file_name.split(separator, 1)[0]
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