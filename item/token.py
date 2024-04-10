from datetime import UTC, datetime
import hashlib
import hmac

import yarl
from item.method_http_request import MethodHttpRequest


class ConfigToken:
    service: str 
    host: str
    access_key: str
    secret_access_key: str
    region_name: str

    def __init__(self, service: str, host: str, access_key: str, secret_access_key: str, region_name: str) -> None:
        self.service = service
        self.host = host
        self.access_key = access_key
        self.secret_access_key = secret_access_key
        self.region_name = region_name

class ConfigUrl:
    method: str
    host: str
    path: str
    params: dict
    
    SSL_certificate: bool = False
    
    def __init__(
        self, method: MethodHttpRequest, host: str, path: str, params: dict = {}, *, SSL_certificate: bool = False
    ) -> None:
        self.method = method.value
        
        self.host = self.encoded_url(host)
        self.path = self.encoded_url(path)
        self.params = self.encoded_params(params)
        
        self.SSL_certificate = SSL_certificate
    
    def _params_to_string(self):
        return '&'.join([f'{k}={v}' for k, v in self.params.items()])
    
    def _protocol(self):
        return 'https' if self.SSL_certificate else 'http'

    def encoded_url(self, text: str):
        return str(yarl.URL(text, encoded=False))
    
    def encoded_params(self, dict_params: dict):
        return {self.encoded_url(k): self.encoded_url(v) for k, v in dict_params.items()}
        
    
    def __str__(self):
        return f'{self._protocol()}://{self.host}{self.path}?{self._params_to_string()}'

    def __call__(self, ) -> str:
        return self.__str__()
    
class Token:

    def __init__(self, config: ConfigToken) -> None:
        self.config = config

    # Функция для создания подписи HMAC.
    def sign(self, key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()
    
    # Функция для получения ключа подписи.
    def get_signature_key(self, key, date_stamp, region_name, service_name):
        # Вычисляем ключ для даты, используя ключ доступа и дату временной метки, затем подписываем его.
        k_date = self.sign(('AWS4' + key).encode('utf-8'), date_stamp)
        # Вычисляем ключ для региона, используя ключ для даты и имя региона, затем подписываем его.
        k_region = self.sign(k_date, region_name)
        # Вычисляем ключ для сервиса, используя ключ для региона и имя сервиса, затем подписываем его.
        k_service = self.sign(k_region, service_name)
        # Вычисляем ключ для подписи, используя ключ для сервиса и строку 'aws4_request'.
        k_signing = self.sign(k_service, 'aws4_request')
        # Возвращаем полученный ключ для подписи.
        return k_signing

    def __call__(self, url: ConfigUrl):
        # method: str, path: str, params: dict
        
        # Указываем канонический путь запроса.
        canonical_uri = url.path
        # Формируем каноническую строку запроса.
        canonical_querystring = url._params_to_string()
        # Формируем канонические заголовки.
        canonical_headers = f'host:{self.config.host}\n'
        # Указываем подписанные заголовки.
        signed_headers = 'host'
        # Вычисляем хеш для тела запроса (в данном случае пусто).
        payload_hash = hashlib.sha256(''.encode('utf-8')).hexdigest()
        # Формируем канонический запрос.
        canonical_request = '\n'.join(
            [
                url.method, 
                canonical_uri, 
                canonical_querystring, 
                canonical_headers, 
                signed_headers, 
                payload_hash
            ]
        )

        # Указываем алгоритм подписи.
        algorithm = 'AWS4-HMAC-SHA256'
        
        # Указываем область учетных данных.
        credential_scope = f'{
            datetime.now(UTC).strftime("%Y%m%d")
        }/{self.config.region_name}/{self.config.service}/aws4_request'
        
        # Формируем строку для подписи.
        string_to_sign = '\n'.join(
            [
                algorithm, 
                datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ"), 
                credential_scope, 
                hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()
            ]
        )

        # Получаем ключ подписи.
        signing_key = self.get_signature_key(
            self.config.secret_access_key, 
            datetime.now(UTC).strftime('%Y%m%d'), 
            self.config.region_name, 
            self.config.service
        )
        # Вычисляем подпись.
        signature = hmac.new(signing_key, (string_to_sign).encode('utf-8'), hashlib.sha256).hexdigest()

        # Формируем заголовок авторизации.
        return f'{algorithm} Credential={self.config.access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}'
