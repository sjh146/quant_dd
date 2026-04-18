# Windows VM Creon Execution Node

이 폴더(`windows_vm_code`) 안의 파일들은 우분투가 아닌 **Windows VM(대신증권 크레온이 설치된 환경)**으로 복사하여 실행해야 합니다.

## ⚙️ 설정 방법 (Windows VM)

1. **Python 설치 (32bit 권장)**
   - 대신증권 API(Cybos Plus)는 32비트 환경에서만 동작하므로, 반드시 **Python 32-bit** 버전을 설치해야 합니다.

2. **의존성 설치**
   명령 프롬프트(cmd)에서 다음을 실행합니다.
   ```cmd
   pip install -r requirements.txt
   ```

3. **Redis 브로커 IP 설정**
   `creon_executor.py` 내부의 `REDIS_URL` 주소를 우분투 머신의 실제 IP 주소로 변경하세요.
   ```python
   # creon_executor.py
   REDIS_URL = os.getenv('REDIS_URL', 'redis://192.168.0.50:6379/0') # <-- 우분투 IP로 수정
   ```
   *(참고: 우분투의 방화벽(UFW 등)에서 6379 포트가 열려있어야 합니다.)*

4. **크레온(Creon Plus) 로그인**
   - 크레온 플러스를 실행하고 **"CYBOS Plus"** 모드로 로그인합니다.
   - 우측 하단 트레이 아이콘에 노란색/초록색 Cybos 아이콘이 떠 있어야 API 통신이 가능합니다.

5. **실행**
   - 관리자 권한으로 명령 프롬프트를 열고 스크립트를 실행합니다. (COM 객체 접근을 위해 관리자 권한 필요)
   ```cmd
   python creon_executor.py
   ```

## 🔄 동작 방식
1. 코드가 실행되면 우분투에 떠 있는 Redis의 `signal.trade` 채널(Pub/Sub)을 실시간으로 구독합니다.
2. AI 시스템(우분투)이 시그널(예: `{"action": "BUY", "symbol": "005930", "quantity": 33}`)을 발송하면,
3. 윈도우 파이썬이 이를 수신하여 `CpTrade.CpTd0311` (주식 주문 COM 객체)를 통해 대신증권 서버로 **시장가 매수(03)** 주문을 꽂아 넣습니다.