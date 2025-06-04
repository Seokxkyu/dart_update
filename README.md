# 수집 피쳐 및 수집 방법

## 개요
DART(금융감독원 전자공시시스템)에서 “단일판매‧공급계약” 공시를 조회해, 지정한 날짜의 공시 정보를 로컬 엑셀 파일에 누적·저장합니다.

- DART의 JSON API로 당일(또는 지정한 날짜)의 공시 리스트를 가져옵니다.
- 각 공시마다 계약 관련 세부 정보를 XML(또는 ZIP 내 XML)에서 파싱합니다.
- `NAVER` 기업정보에서 해당 기업의 업종 분류(WICS)와 시가총액 정보를 크롤링합니다.
- 로컬 엑셀 파일에 중복되지 않는 신규 레코드만 “날짜, 공시회사, 계약 금액” 기준으로 필터링하여 추가합니다.


## 주요 기능 및 함수
### 1. `fetch_sales(session, target_date: str)`
- DART API에서 지정한 target_date(YYYYMMDD)의 공시 목록을 JSON으로 가져옵니다.
- report_nm에 “단일판매”가 포함되고 “정정” 또는 “해지”가 포함되지 않은 공시만 필터링합니다.
- 반환 컬럼 예시: rcept_no, corp_name, rcept_dt, stock_code, corp_cls, report_nm 등.
- 반환값: 필터링된 DataFrame.

### 2. `parse_contract(session, rcept_no: str)`
- DART “공시문서” (XML 또는 ZIP 내 XML/HTML)를 가져와 파싱합니다.
- 테이블에서 “체결계약명”, “계약금액”, “시작일”, “종료일”, “계약상대” 항목을 찾아서 텍스트로 추출합니다.
- **반환값(딕셔너리)**:
   - `내용` : 계약명 또는 판매·공급 계약 내용 (문자열)
   - `계약 금액(억)` : 계약 금액(억 단위, 실수)
   - `매출액 대비(%) (A)` : 계약 금액 대비 매출 비율(실수)
   - `시작일 (s)` : 계약 시작일(YYYY-MM-DD 문자열)
   - `종료일 (e)` : 계약 종료일(YYYY-MM-DD 문자열)
   - `계약상대` : 상대 회사명(문자열)
- ZIP이 아니거나 테이블을 찾지 못하면 빈 딕셔너리 {} 반환.

### 3. `fetch_market_info(session, stock_code: str)`
- `NAVER` 기업정보 페이지에서 WICS(업종 분류)와 시가총액(억원)을 크롤링합니다.
- **반환값(딕셔너리)**:
   - `업종 분류` : WICS 문자열(예: “반도체와반도체장비”)
   - `시가총액(억)` : 시가총액(억 단위, 정수) 또는 None

### 4. `filter_new_rows(result_df: pd.DataFrame, existing_df: pd.DataFrame)`
- `result_df`와 기존 엑셀에서 읽은 `existing_df`를 받아,
“날짜 (D)”, “공시회사”, “계약 금액(억)” 3개 컬럼을 기준으로 중복 여부를 비교해 신규 레코드만 반환합니다.
- `existing_df`가 비어 있으면 `result_df` 전체를 복사해 반환합니다.

### `5. update_excel(result_df: pd.DataFrame, excel_path: str)`
- 로컬 엑셀(excel_path)이 있으면 로드, 없으면 새 워크북 생성.
- 워크북에 main 시트가 있으면 해당 시트 가져오기, 없으면 만든 뒤 첫 행에 헤더(컬럼명) 삽입.
- 기존 main 시트에서 `날짜 (D)`, `공시회사`, `계약 금액(억)` 3개 컬럼만 읽어 existing_df를 만듭니다.
- filter_new_rows로 중복 제거 후, new_rows만 아래에 append 합니다.
- 추가된 행 셀에 대해:
   - `날짜 (D)`, `시작일 (s)`, `종료일 (e)` → number_format='yyyy-mm-dd'
   - 숫자형(정수/실수) → 정수: `#,##0`, 실수: `#,##0.00`
   - “내용”/“계약상대” 제외 모든 셀 → 가운데 정렬
- 저장: `wb.save(excel_path)`

### `6. main(target_date: str, excel_path: str)`
1. `fetch_sales(session, target_date)` 호출해 당일(또는 지정날짜) 공시 리스트 조회.
   - 공시 없다면 메시지 출력 후 종료.
2. 종목 코드 리스트(`stock_code`)를 추출해 `fetch_market_info`로 `market_infos` 딕셔너리 구축.

3. 각 공시 레코드마다 `parse_contract` 호출해 계약 세부 정보를 파싱.
   - `업종 분류(WICS)`가 “건설”이면 건너뜁니다.

4. 파싱 결과와 `market_infos`를 합쳐 `records` 리스트 생성.

5. `records`를 DataFrame(result_df)으로 변환 후:
   - `날짜 (D)`, `시작일 (s)`, `종료일 (e)` → `pd.to_datetime`
   - 숫자형 컬럼(`계약 금액(억)`, `매출액 대비(%) (A)`, `시가총액(억)`)이 object dtype이면 쉼표 제거 후 `astype(float/int)`로 변환.

6. `update_excel(result_df, excel_path)`로 엑셀 업데이트.

7. 완료 메시지 출력.

본 스크립트는 DART Open API와 네이버 컴퍼니 리포트를 활용해, 오늘자 단일판매·공급계약 공시 정보를 수집하고 아래 피쳐들을 생성합니다.

| 컬럼명               | 설명                                               |
|---------------------|----------------------------------------------------|
| 공시회사            | 공시를 등록한 회사 이름 (`corp_name`)               |
| 날짜 (D)            | 공시 일자 (`YYYYMMDD` → `yyyy-mm-dd`)              |
| 거래소              | 거래소 구분 (`KS` 또는 `KQ`)                        |
| 내용                | 계약/판매 내용 (`parse_contract` 결과)              |
| 계약 금액(억)       | 계약 총액(억 단위, 실수)                            |
| 매출액 대비(%) (A)   | 계약 금액이 매출 대비 몇 %인지 (실수)               |
| 계약상대            | 계약 상대방 회사명                                  |
| 시작일 (s)          | 계약 시작일 (`YYYY-MM-DD`)                          |
| 종료일 (e)          | 계약 종료일 (`YYYY-MM-DD`)                          |
| 업종 분류            | WICS 업종 분류 (예: “반도체와반도체장비” 등)         |
| 시가총액(억)        | 해당 회사 시가총액(억 단위, 정수)                    |


<!-- | 피쳐                    | 설명                                    | 수집처 & 방법                                                                                                               |
|-------------------------|-----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| **stock_code**          | 6자리 종목코드                          | DART Open API `list.json` (`https://opendart.fss.or.kr/api/list.json`) 응답의 `stock_code` 필드                                 |
| **corp_name**           | 공시 회사명                             | DART Open API `list.json` 응답의 `corp_name` 필드                                                                           |
| **announcement_date**   | 공시일자 (`YYYY-MM-DD`)                | DART API `list.json` 응답의 `rcept_dt` (`YYYYMMDD`) → `pd.to_datetime(..., format='%Y%m%d')`                                    |
| **contract_name**       | 체결 계약명                             | DART 상세공시 문서 (`https://opendart.fss.or.kr/api/document.xml?crtfc_key=...&rcept_no=...`) 내 ZIP 압축 해제 후 나온 HTML `<table>`에서 `체결계약명` 레이블 뒤 `<td>` 텍스트 |
| **contract_amount**     | 계약금액 (정수, 원 단위)                | 위 DART 상세공시 HTML 테이블에서 `계약금액` 레이블 뒤 `<td>` 텍스트 → 쉼표 제거 → `int()` 변환                                  |
| **recent_sales_amount** | 최근 매출액 (정수, 원 단위)            | DART 상세공시 HTML 테이블에서 `최근매출액` 레이블 뒤 `<td>` 텍스트 → 쉼표 제거 → `int()` 변환                                |
| **sales_ratio_percent** | 매출액 대비 계약금액 비율 (%)           | DART 상세공시 HTML 테이블에서 `매출액대비` 레이블 뒤 `<td>` 텍스트 → `float()` 변환                                           |
| **contract_date**       | 계약(수주)일자 (`YYYY-MM-DD`)          | DART 상세공시 HTML 테이블에서 `계약(수주)일자` 레이블 뒤 `<td>` 텍스트 (`YYYY-MM-DD`) → `pd.to_datetime(..., format='%Y-%m-%d')` |
| **WICS**                | WICS 업종명                             | 네이버 컴퍼니 리포트 (`https://navercomp.wisereport.co.kr/v2/company/c1010001.aspx?cmp_cd=...`) HTML `<dt>` 중 `WICS:` 텍스트 |
| **market_cap**          | 시가총액 (문자열, 억원 단위 콤마)       | 같은 네이버 리포트 페이지 `<table id="cTB11">` 내 `th.txt`에서 “시가총액” 검출 → 해당 행 `td.num` 텍스트                        |

---

## 간단한 실행 흐름

1. **오늘자 공시 목록 조회**  
   – `fetch_sales(session)` 호출 → DART `list.json`  
2. **관심 종목 필터링**  
   – 전역 `TARGET_STOCKS` 리스트에 포함된 종목만 선별  
3. **상세 계약 정보 파싱**  
   – 각 `rcept_no` 별로 DART 상세공시 문서(`document.xml`) 요청 → ZIP 해제 후 HTML 테이블에서 주요 값 추출  
4. **시장정보 수집 & 캐싱**  
   – 네이버 컴퍼니 리포트 페이지 요청 → WICS, 시가총액 파싱  
5. **DataFrame 조립 및 타입 변환**  
   – `records` → `pd.DataFrame` 생성 → 날짜 컬럼(datetime) 변환  
6. **결과 출력**  
   – 콘솔 또는 CSV 저장   -->
