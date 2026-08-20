from typing import Optional, Dict
import gcsfs


# K -> manifest dosyasının "_" öncesindeki ismi
K_MANIFEST_PREFIX = {
    "k1": "iris",
    "k2": "abc",
    "k3": "xyz",
    "k4": "veraset",
}


def find_manifest(folder_name: str, k_suffix: str) -> Optional[Dict[str, str]]:
    """
    Verilen GCS folder içerisinde ilgili K için manifest'i bulur.

    Örnek:
        folder:
            gs://arcanor-orion/output/mobility/TUR/2026/08/13/

        k1:
            iris

        aranacak:
            gs://.../iris_batch_manifest.txt

    Return:
        {
            "folder_name": "...",
            "file_name": "iris",
            "manifest_path": "gs://.../iris_batch_manifest.txt"
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

        # Örneğin:
        # iris_batch_manifest.txt
        #
        # "_" öncesi:
        # iris
        if "_" not in file_name:
            continue

        manifest_prefix = file_name.split("_", 1)[0]

        if manifest_prefix != expected_prefix:
            continue

        if not file_name.endswith("manifest.txt"):
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