"""
EBCDIC Conversion Agent
Convert legacy source files from EBCDIC encoding to ASCII-safe text.
"""

import logging
from pathlib import Path
from typing import List, Optional

logger = logging.getLogger(__name__)

DEFAULT_EBCDIC_ENCODINGS = [
    "cp037",
    "cp500",
    "cp1047",
    "cp1140",
    "cp273",
]


def text_to_ebcdic(text, ebcdic_codec="cp037"):
    return text.encode(ebcdic_codec)


def ebcdic_to_ascii(ebcdic_bytes, ebcdic_codec="cp037"):
    # Decode EBCDIC bytes to normal text
    normal_text = ebcdic_bytes.decode(ebcdic_codec)

    # Encode normal text to ASCII bytes
    ascii_bytes = normal_text.encode("ascii")
    return ascii_bytes


def ascii_to_text(ascii_bytes):
    return ascii_bytes.decode("ascii")


def bytes_to_hex(data):
    return " ".join(f"{b:02X}" for b in data)


def main():
    input_text = input("Enter text: ")

    try:
        # Step 1: Text -> EBCDIC
        ebcdic_bytes = text_to_ebcdic(input_text)

        # Step 2: EBCDIC -> ASCII
        ascii_bytes = ebcdic_to_ascii(ebcdic_bytes)

        # Step 3: ASCII -> Normal text
        final_text = ascii_to_text(ascii_bytes)

        # Validation
        is_valid = input_text == final_text

        print("\n--- Conversion Results ---")
        print(f"Original Text       : {input_text}")
        print(f"EBCDIC Bytes (hex)  : {bytes_to_hex(ebcdic_bytes)}")
        print(f"ASCII Bytes (hex)   : {bytes_to_hex(ascii_bytes)}")
        print(f"Final Normal Text   : {final_text}")
        print(f"Validation Result   : {'MATCH' if is_valid else 'MISMATCH'}")

    except UnicodeEncodeError as e:
        print("\nError: The input contains characters that cannot be encoded in ASCII.")
        print("ASCII supports only basic English characters and symbols.")
        print(f"Details: {e}")

    except Exception as e:
        print("\nUnexpected error occurred.")
        print(f"Details: {e}")


if __name__ == "__main__":
    main()


class EBCDICConverter:
    """Convert text and files between EBCDIC and ASCII formats."""

    def __init__(self, ebcdic_codec: str = "cp037"):
        self.ebcdic_codec = ebcdic_codec
        logger.info(f"EBCDICConverter initialized with codec: {self.ebcdic_codec}")

    def convert_bytes_to_ascii(self, data: bytes) -> str:
        """Convert EBCDIC bytes to ASCII text"""
        try:
            ascii_bytes = ebcdic_to_ascii(data, self.ebcdic_codec)
            # Return as string
            return ascii_to_text(ascii_bytes)
        except Exception as e:
            logger.error(f"Conversion failed: {e}")
            raise

    def convert_file_to_ascii(self, source_file: Path, output_file: Optional[Path] = None) -> Path:
        """Convert a file from EBCDIC to ASCII"""
        if output_file is None:
            output_file = source_file

        data = source_file.read_bytes()
        ascii_text = self.convert_bytes_to_ascii(data)

        output_file.parent.mkdir(parents=True, exist_ok=True)
        output_file.write_text(ascii_text, encoding="ascii", errors="replace")
        logger.info(f"Converted {source_file} to ASCII at {output_file}")
        return output_file

    def convert_directory(self, source_dir: Path, output_dir: Path) -> List[Path]:
        """Convert all files in a directory from EBCDIC to ASCII"""
        output_dir.mkdir(parents=True, exist_ok=True)
        converted_paths: List[Path] = []

        for source_path in source_dir.rglob("*"):
            if not source_path.is_file():
                continue

            target_path = output_dir / source_path.relative_to(source_dir)
            target_path.parent.mkdir(parents=True, exist_ok=True)

            try:
                self.convert_file_to_ascii(source_path, target_path)
            except Exception as exc:
                logger.warning(f"Could not convert {source_path}: {exc}")
                # Fallback: copy raw bytes when conversion fails.
                target_path.write_bytes(source_path.read_bytes())

            converted_paths.append(target_path)

        return converted_paths

