import time
import requests
import pandas as pd
import ta
import jwt
import uuid
import hashlib
from urllib.parse import urlencode
from ta.momentum import RSIIndicator

# 업비트 API 키
access_key = 'AK'
secret_key = 'SK'

# 업비트 API URL
base_url = 'https://api.upbit.com/v1'

# 매수/매도 조건
RSI_LOWER_THRESHOLD = 30
RSI_UPPER_THRESHOLD = 70

# 시장과 거래할 코인 설정 (예: KRW-BTC)
market = 'KRW-BTC'

# 슬랙 웹훅 URL (알림용, 필요시 사용)
slack_webhook_url = 'URL'

# 슬랙 메시지 전송 함수
def send_slack_message(message):
    payload = {'text': message}
    requests.post(slack_webhook_url, json=payload)

# 업비트 API 요청 함수
def send_signed_request(url, method='GET', data=None):
    query_string = urlencode(data) if data else ''
    m = hashlib.sha512()
    m.update(query_string.encode())
    query_hash = m.hexdigest()

    payload = {
        'access_key': access_key,
        'nonce': str(uuid.uuid4()),
        'query_hash': query_hash,
        'query_hash_alg': 'SHA512'
    }

    jwt_token = jwt.encode(payload, secret_key, algorithm='HS256')
    authorize_token = 'Bearer {}'.format(jwt_token)
    headers = {'Authorization': authorize_token}

    if method == 'POST':
        res = requests.post(url, json=data, headers=headers)
    else:
        res = requests.get(url, headers=headers)
    
    return res.json()

# 잔액 확인 함수
def get_balance(currency):
    url = f'{base_url}/accounts'
    accounts = send_signed_request(url)
    for account in accounts:
        if account['currency'] == currency:
            return float(account['balance'])
    return 0.0

# 캔들 데이터 가져오기
def get_candles(market, minutes, count=200):
    url = f'{base_url}/candles/minutes/{minutes}?market={market}&count={count}'
    response = requests.get(url)
    return response.json()

# RSI 계산 함수
def calculate_rsi(df, period=14):
    rsi_indicator = RSIIndicator(close=df['close'], window=period)
    df['rsi'] = rsi_indicator.rsi()
    return df

# 마켓 정보 가져오기
def get_market_info():
    url = f'{base_url}/market/all'
    response = requests.get(url)
    return response.json()

# 특정 마켓의 최소 주문 단위 확인 함수
def get_min_order_size(market):
    market_info = get_market_info()
    for m in market_info:
        if m['market'] == market:
            return m.get('min_total', 5000)  # 기본값을 5000 KRW로 설정 (업비트의 경우)
    return None

# 매수 함수
def buy(market):
    krw_balance = get_balance('KRW')
    current_price = get_current_price(market)
    spend_amount = krw_balance * 0.6  # 사용할 금액은 잔액의 60%
    volume = spend_amount / current_price
    
    min_order_size = get_min_order_size(market)
    if spend_amount < min_order_size:
        send_slack_message(f"Order size is less than the minimum order size: {min_order_size} KRW.")
        raise SystemExit(f"Order size is less than the minimum order size: {min_order_size} KRW.")
    
    url = f'{base_url}/orders'
    data = {
        'market': market,
        'side': 'bid',
        'volume': str(volume),
        'ord_type': 'price',
        'price': str(current_price)
    }
    response = send_signed_request(url, 'POST', data)
    send_slack_message(f"Bought {volume} of {market} at {current_price} KRW")
    return response

# 매도 함수
def sell(market):
    coin_balance = get_balance(market.split('-')[1])
    current_price = get_current_price(market)
    volume = coin_balance * 0.7  # 매도할 코인 양은 잔액의 70%
    
    min_order_size = get_min_order_size(market)
    if (volume * current_price) < min_order_size:
        send_slack_message(f"Order size is less than the minimum order size: {min_order_size} KRW.")
        raise SystemExit(f"Order size is less than the minimum order size: {min_order_size} KRW.")
    
    url = f'{base_url}/orders'
    data = {
        'market': market,
        'side': 'ask',
        'volume': str(volume),
        'ord_type': 'market'
    }
    response = send_signed_request(url, 'POST', data)
    send_slack_message(f"Sold {volume} of {market}")
    return response

# 현재 가격 가져오기
def get_current_price(market):
    url = f'{base_url}/ticker?markets={market}'
    response = requests.get(url)
    return float(response.json()[0]['trade_price'])

# 메인 함수
def main():
    min_order_size = get_min_order_size(market)
    if min_order_size:
        print(f"Minimum order size for {market}: {min_order_size} KRW")
    else:
        print(f"Could not find minimum order size for {market}")

    while True:
        try:
            candles = get_candles(market, 3)
            df = pd.DataFrame(candles)
            df = df[['candle_date_time_kst', 'opening_price', 'high_price', 'low_price', 'trade_price']]
            df.columns = ['timestamp', 'open', 'high', 'low', 'close']
            df = df.sort_values(by='timestamp').reset_index(drop=True)
            
            df = calculate_rsi(df)
            latest_rsi = df.iloc[-1]['rsi']
            print(f"Latest RSI: {latest_rsi}")

            # 매수 조건
            if latest_rsi <= RSI_LOWER_THRESHOLD:
                buy(market)

            # 매도 조건
            elif latest_rsi >= RSI_UPPER_THRESHOLD:
                sell(market)

            time.sleep(180)  # 3분 대기

        except Exception as e:
            print(f"Error: {e}")
            send_slack_message(f"Error: {e}")
            time.sleep(180)  # 에러 발생 시 3분 대기

if __name__ == "__main__":
    while True:
        try:
            main()
        except Exception as e:
            print(f"Critical Error: {e}")
            send_slack_message(f"Critical Error: {e}")
            time.sleep(300)  # 치명적 에러 발생 시 5분 대기 후 재시작
