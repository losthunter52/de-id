from Crypto.Cipher import AES, ChaCha20, Salsa20
from Crypto.Util.Padding import pad
from Crypto.Random import get_random_bytes
import hashlib

def encrypt_chacha20(df, columns, semaphore, **configuration):
    """
    Encrypts the values in the specified columns of the DataFrame using ChaCha20 cipher.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        columns (list): The list of column names to encrypt.
        key (str): The encryption key.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """

    key = configuration.get('key')
    if not key:
        raise ValueError("Encryption key not provided in the configuration.")


    key_derived = hashlib.sha256(key.encode()).digest()[:32]  # Derive a 256-bit key from the provided key using SHA-256
    nonce = get_random_bytes(8)  # Generate a random 64-bit nonce
    cipher = ChaCha20.new(key=key_derived, nonce=nonce)  # Create an instance of the ChaCha20 cipher with the derived key and generated nonce

    def encrypt_value(value):
        return cipher.encrypt(value.encode())  # Encrypt the value using the ChaCha20 cipher

    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in columns:
        df[column] = df[column].apply(encrypt_value)  # Apply encryption to all rows of the specified column
        df[f'{column}_nonce'] = nonce  # Store the nonce in the encrypted DataFrame for later use in decryption
    semaphore.release()  # Release the semaphore after modifying the DataFrame


def encrypt_aes(df, columns, semaphore, **configuration):
    """
    Encrypts the values in the specified columns of the DataFrame using AES cipher.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        columns (list): The list of column names to encrypt.
        key (str): The encryption key.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """
    key = configuration.get('key')
    if not key:
        raise ValueError("Encryption key not provided in the configuration.")

    key_derived = hashlib.sha256(key.encode()).digest()  # Derive a 256-bit key from the provided key using SHA-256
    cipher = AES.new(key_derived, AES.MODE_ECB)  # Create an instance of the AES cipher with the derived key

    def encrypt_value(value):
        value_padded = pad(value.encode(), AES.block_size)  # Pad the value to have a size multiple of the AES block size
        return cipher.encrypt(value_padded)  # Encrypt the value using the AES cipher

    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in columns:
        df[column] = df[column].apply(encrypt_value)  # Apply encryption to all rows of the specified column
    semaphore.release()  # Release the semaphore after modifying the DataFrame


def encrypt_salsa20(df, columns, semaphore, **configuration):
    """
    Encrypts the values in the specified columns of the DataFrame using Salsa20 cipher.

    Args:
        df (pandas.DataFrame): The input DataFrame.
        columns (list): The list of column names to encrypt.
        key (str): The encryption key.
        semaphore (threading.Semaphore): Semaphore to synchronize access to the DataFrame.
    """

    key = configuration.get('key')
    if not key:
        raise ValueError("Encryption key not provided in the configuration.")

    key_derived = hashlib.sha256(key.encode()).digest()[:32]  # Derive a 256-bit key from the provided key using SHA-256
    nonce = get_random_bytes(8)  # Generate a random 64-bit nonce
    cipher = Salsa20.new(key=key_derived, nonce=nonce)  # Create an instance of the Salsa20 cipher with the derived key and generated nonce

    def encrypt_value(value):
        return cipher.encrypt(value.encode())  # Encrypt the value using the Salsa20 cipher

    semaphore.acquire()  # Acquire the semaphore before modifying the DataFrame
    for column in columns:
        df[column] = df[column].apply(encrypt_value)  # Apply encryption to all rows of the specified column
        df[f'{column}_nonce'] = nonce  # Store the nonce in the encrypted DataFrame for later use in decryption
    semaphore.release()  # Release the semaphore after modifying the DataFrame
