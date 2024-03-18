import boto3
from flask_caching.backends.cache import BaseCache
import logging
import io
import pickle


class S3Cache(BaseCache):
    def __init__(self,
                 bucket_name: str,
                 key_prefix: str,
                 default_timeout=300,
                 extra_s3_args: dict = {}
                 ):
        self.default_timeout = default_timeout

        self.s3_client = boto3.client('s3', **extra_s3_args)

        self.bucket_name = bucket_name
        self.key_prefix = key_prefix

    def _get_full_key(self, key):
        """ Generate a S3 key from the cache key, append the prefix. """
        return f"{self.key_prefix}{key}"

    def _key_exists(self, key) -> bool:
        """ If a file with this key exists, then the key exists
            head_object throws an exception when the key doesn't exist
        """
        try:
            self.s3_client.head_object(
                Bucket=self.bucket_name,
                Key=self._get_full_key(key)
            )
        except Exception:
            return False
        else:
            return True

    def get(self, key: str):
        """Look up key in the cache and return the value for it.

        :param key: the key to be looked up.
        :returns: The value if it exists and is readable, else ``None``.
        """
        if not self._key_exists(key):
            return None
        else:
            value_file = io.BytesIO()

            try:
                self.s3_client.download_fileobj(
                    self.bucket_name,
                    self._get_full_key(key),
                    value_file
                )
            except Exception as e:
                logging.warn("Failed to get key {key}")
                logging.exception(e)

                return None
            else:
                value_file.seek(0)
                return pickle.load(value_file)

    def delete(self, key: str) -> bool:
        """Delete `key` from the cache.

        :param key: the key to delete.
        :returns: Whether the key existed and has been deleted.
        :rtype: boolean
        """
        if not self._key_exists(key):
            return False
        else:
            try:
                self.s3_client.delete_object(
                    self.bucket_name,
                    self._get_full_key(key)
                )
            except Exception as e:
                logging.warn("Failed to delete key {key}")
                logging.exception(e)

                return False
            else:
                return True

    def set(self, key: str, value, timeout=None):
        """Add a new key/value to the cache.

        If the key already exists, the existing value is overwritten.

        :param key: the key to set
        :param value: the value for the key
        :param timeout: the timeout value is ignored on S3Cache,
                        use lifecycle policies instead
        :returns: ``True`` if key has been updated, ``False`` for backend
                  errors. Pickling errors, however, will raise a subclass of
                  ``pickle.PickleError``.
        :rtype: boolean
        """

        # Pickle is a serde library, in this case we want to serialize value
        # and dump it to an in memory buffer
        # initialize and open the buffer
        value_file = io.BytesIO()
        # write the serialized value to the buffer
        # this sets the buffer position to the end of the buffer
        pickle.dump(value, value_file)

        try:
            # reset buffer position to the beginning
            value_file.seek(0)
            # use upload_fileobj to upload the in-memory buffer
            self.s3_client.upload_fileobj(
                value_file,
                self.bucket_name,
                self._get_full_key(key))
        except Exception as e:
            logging.warn("Error while setting key {key}")
            logging.exception(e)

            return False
        else:
            return True

    def add(self, key: str, value, timeout=None):
        """Works like :meth:`set` but does not overwrite the values of already
        existing keys.

        :param key: the key to set
        :param value: the value for the key
        :param timeout: the cache timeout for the key in seconds (if not
        specified, it uses the default timeout). A timeout of
        0 indicates that the cache never expires.
        :returns: Same as :meth:`set`, but also ``False`` for already
                  existing keys.
        :rtype: boolean
        """
        if self._key_exists(key):
            return False
        else:
            return self.set(key, value, timeout=timeout)

    def clear(self, key: str):
        """Clears the cache.  Keep in mind that not all caches support
        completely clearing the cache.

        :returns: Whether the cache has been cleared.
        :rtype: boolean
        """
        return False

    def has(self, key: str):
        """Checks if a key exists in the cache without returning it. This is a
        cheap operation that bypasses loading the actual data on the backend.

        :param key: the key to check
        """
        self._key_exists(key)
