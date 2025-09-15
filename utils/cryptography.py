from Cryptodome.Cipher import AES
from Cryptodome.Random import get_random_bytes
import pandas as pd
import base64
import sys
import os
sys.path.append(os.getcwd())

from config import ENCODING, SECURITY

class ColumnEncryptor:
    def __init__(self, key: str = SECURITY['key'], 
                 iv: int = SECURITY['initial_vector']):
        
        self.key = self._adjust_key_length(key.encode(ENCODING))
        self.iv = iv
        self.df = pd.DataFrame()

    def _adjust_key_length(self, key: bytes) -> bytes:
        """
        Adjust the key length to be either 16, 24, or 32 bytes.
        """
        if len(key) not in [16, 24, 32]:
            if len(key) < 16:
                key = key.ljust(16, b'\0')
            elif len(key) < 24:
                key = key.ljust(24, b'\0')
            elif len(key) < 32:
                key = key.ljust(32, b'\0')
            else:
                key = key[:32]
        return key

    def encrypt_column(self, column: pd.Series) -> pd.Series:
        """
        Encrypts a column (pandas Series) and returns the encrypted data.

        Arguments:
        column (pd.Series): Name of the column with the data to be encrypted.

        Returns:
            pd.Series with the encrypted data.
        """
        if not isinstance(column, pd.Series):
            raise ValueError(f"'{column} should be a pandas series'")

        encrypted_data = []
        for data in column:
            data_bytes = data.encode(ENCODING)
            initial_vector = get_random_bytes(self.iv)
            cipher = AES.new(self.key, AES.MODE_GCM, nonce=initial_vector)
            padded_data = self._pad(data_bytes, AES.block_size)
            ciphertext, tag = cipher.encrypt_and_digest(padded_data)

            encrypted_data.append(
                base64.b64encode(initial_vector + ciphertext + tag).decode(ENCODING)
            )

        return pd.Series(encrypted_data)

    def decrypt_column(self, column: pd.Series) -> pd.Series:
        """
        Decrypts a column (pandas Series) and returns the decrypted data.

        Arguments:
        column (pd.Series): Name of the column with the encrypted data.

        Returns:
            pd.Series with the decrypted data.
        """
        if not isinstance(column, pd.Series):
            raise ValueError(f"'{column} should be a pandas series'")

        decrypted_data = []
        for data in column:
            data_bytes = base64.b64decode(data.encode(ENCODING))
            initial_vector = data_bytes[:self.iv]
            ciphertext = data_bytes[self.iv:-16]
            tag = data_bytes[-16:]

            cipher = AES.new(self.key, AES.MODE_GCM, nonce=initial_vector)
            padded_data = cipher.decrypt_and_verify(ciphertext, tag)
            decrypted_data.append(self._unpad(padded_data).decode(ENCODING))

        return pd.Series(decrypted_data)

    def _pad(self, data: bytes, block_size: int) -> bytes:
        padding_length = block_size - len(data) % block_size
        return data + bytes([padding_length] * padding_length)

    def _unpad(self, padded_data: bytes) -> bytes:
        padding_length = padded_data[-1]
        return padded_data[:-padding_length]

if __name__ == "__main__":
    encryptor = ColumnEncryptor()
    sample_data = pd.Series(['data1', 'data2', 'data3'])
    encrypted = encryptor.encrypt_column(sample_data)
    print(encrypted)
    decrypted = encryptor.decrypt_column(encrypted)
    print(decrypted)