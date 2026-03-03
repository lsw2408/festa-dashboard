"""
리빙 페스타 실적 비교 대시보드
- 11월 블랙위크, 12월 리빙페스타, 2월 리빙페스타 3개 행사 비교
- Plotly 기반 인터랙티브 HTML 대시보드 생성
"""

import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import plotly.express as px
import json
import re
import os
import pickle
from pathlib import Path

# =============================================================================
# Google Sheets 전체 데이터 로드 (브랜드 분석용)
# =============================================================================

# Spreadsheet ID 및 시트명
SPREADSHEET_ID = "1xV4Dke6KItEE_JfVJXn_16XFr_5xQlqTyRN5v_NghYY"
SHEET_NAMES = {
    "nov": "11월_블랙위크 실적",
    "dec": "12월_리빙페스타 실적",
    "feb": "2월_리빙페스타 실적",
}

# 인증 파일 경로
# GitHub Actions(CI)에서는 환경변수로 경로를 지정, 로컬에서는 기존 경로 사용
_SCRIPT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))
TOKEN_PATH = Path(os.environ.get(
    "SHEETS_TOKEN_PATH",
    "/Users/bette/claude_code/morning_briefing/credentials/token_sheets.pickle"
))
CREDENTIALS_PATH = Path(os.environ.get(
    "SHEETS_CREDENTIALS_PATH",
    "/Users/bette/claude_code/morning_briefing/credentials/google_credentials.json"
))


def get_sheets_credentials():
    """morning_briefing 프로젝트의 OAuth token을 재활용하여 인증 정보 반환.

    token이 만료되었으면 자동 갱신(refresh)하고, 갱신 실패 시 None 반환.
    """
    try:
        from google.oauth2.credentials import Credentials
        from google.auth.transport.requests import Request

        if not TOKEN_PATH.exists():
            print(f"[Sheets] token 파일 없음: {TOKEN_PATH}")
            return None

        with open(TOKEN_PATH, "rb") as f:
            creds = pickle.load(f)

        # token 만료 시 갱신
        if creds and creds.expired and creds.refresh_token:
            print("[Sheets] token 만료 → 갱신 중...")
            creds.refresh(Request())
            with open(TOKEN_PATH, "wb") as f:
                pickle.dump(creds, f)
            print("[Sheets] token 갱신 완료")

        return creds
    except Exception as e:
        print(f"[Sheets] 인증 실패: {e}")
        return None


def get_sheet_dates(sheet_name, service):
    """시트의 B열(ord_dt)에서 고유 날짜 목록을 오름차순으로 반환."""
    range_str = f"'{sheet_name}'!B3:B"
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_str,
    ).execute()
    rows = result.get("values", [])
    dates = set()
    for row in rows:
        if row and row[0].strip():
            dates.add(row[0].strip())
    return sorted(dates)


def fetch_sheet_brand_data(sheet_name, service, allowed_dates=None):
    """한 시트의 A3:I 범위를 읽어 지정된 날짜들만 필터링한 DataFrame 반환.

    Args:
        sheet_name: 시트 이름 (예: '11월_블랙위크 실적')
        service: Google Sheets API service 객체
        allowed_dates: 허용할 날짜 목록 (예: ['1117', '1118']). None이면 전체 반환.

    Returns:
        pandas DataFrame (컬럼: ord_dt, 대카테고리, 콘텐츠 상품명, 브랜드명, 소싱유형, 판매수량, gmv2)
    """
    # B열=ord_dt, C열=대카테고리, E열=콘텐츠 상품명, F열=브랜드명, G열=소싱유형, H열=판매수량, I열=gmv2
    range_str = f"'{sheet_name}'!A3:I"
    result = service.spreadsheets().values().get(
        spreadsheetId=SPREADSHEET_ID,
        range=range_str,
    ).execute()

    rows = result.get("values", [])
    if not rows:
        print(f"[Sheets] '{sheet_name}' 데이터 없음")
        return pd.DataFrame(columns=["ord_dt", "대카테고리", "콘텐츠 상품명", "브랜드명", "소싱유형", "판매수량", "gmv2"])

    # 허용 날짜를 set으로 변환 (빠른 검색용)
    allowed_set = set(allowed_dates) if allowed_dates else None

    records = []
    filtered_count = 0
    for row in rows:
        # A~I = 9개 컬럼, 최소 F~I(인덱스 5~8)가 있어야 함
        if len(row) < 9:
            continue
        # 날짜 필터링: B열(인덱스1, ord_dt)이 허용 날짜 목록에 포함된 행만 사용
        if allowed_set is not None:
            ord_dt = str(row[1]).strip()
            if ord_dt not in allowed_set:
                filtered_count += 1
                continue
        # 각 열 추출
        ord_dt_val = str(row[1]).strip() if len(row) > 1 and row[1] else ""
        category = row[2].strip() if len(row) > 2 and row[2] else ""
        product_name = row[4].strip() if len(row) > 4 and row[4] else ""
        brand = row[5].strip() if row[5] else ""
        sourcing = row[6].strip() if row[6] else ""
        # 판매수량과 gmv2는 숫자로 변환 (쉼표 제거)
        try:
            qty = int(str(row[7]).replace(",", ""))
        except (ValueError, IndexError):
            qty = 0
        try:
            gmv2 = int(str(row[8]).replace(",", ""))
        except (ValueError, IndexError):
            gmv2 = 0
        if brand:  # 브랜드명이 있는 행만
            records.append({
                "ord_dt": ord_dt_val,
                "대카테고리": category,
                "콘텐츠 상품명": product_name,
                "브랜드명": brand,
                "소싱유형": sourcing,
                "판매수량": qty,
                "gmv2": gmv2,
            })

    df = pd.DataFrame(records)
    if allowed_set:
        print(f"[Sheets] '{sheet_name}' {len(allowed_set)}일치({','.join(sorted(allowed_set))}) → {len(df)}개 상품 로드 ({filtered_count}행 제외)")
    else:
        print(f"[Sheets] '{sheet_name}' → {len(df)}개 상품 로드")
    return df


def fetch_daily_gmv_summary(service):
    """2월_리빙페스타 실적 시트의 T열에서 일자별 GMV 요약 데이터를 읽어 반환.

    시트의 S11:U 영역에 일자별 GMV2와 누적 GMV2가 정리되어 있음.
    (S열: 일자, T열: GMV2, U열: 누적 GMV2)

    Returns:
        {"daily_gmv": [일별 GMV 리스트], "cumulative": [누적 GMV 리스트],
         "dates": [날짜 리스트], "total": 총 누적 GMV} 또는 실패 시 None
    """
    try:
        # S11:U 범위를 넉넉하게 읽기 (최대 30일까지 대응)
        range_str = f"'{SHEET_NAMES['feb']}'!S11:U50"
        result = service.spreadsheets().values().get(
            spreadsheetId=SPREADSHEET_ID,
            range=range_str,
        ).execute()
        rows = result.get("values", [])
        if not rows:
            print("[Sheets] T열 일자별 요약 데이터 없음")
            return None

        # 첫 행은 제목("2월 리빙페스타 기간 GMV"), 두 번째 행은 헤더("일자", "GMV2", "누적 GMV2")
        # 세 번째 행부터 실제 데이터
        daily_gmv = []
        cumulative = []
        dates = []
        for row in rows:
            # 데이터 행: S열에 날짜(예: "2/23"), T열에 GMV2, U열에 누적 GMV2
            if len(row) < 3:
                continue
            date_val = str(row[0]).strip()
            # 날짜 형식 확인 ("2/23", "3/1" 등 또는 "0223" 형식)
            if not date_val or date_val in ("일자", "GMV2", "누적 GMV2", "2월 리빙페스타 기간 GMV"):
                continue
            try:
                gmv_val = int(str(row[1]).replace(",", ""))
                cum_val = int(str(row[2]).replace(",", ""))
                dates.append(date_val)
                daily_gmv.append(gmv_val)
                cumulative.append(cum_val)
            except (ValueError, IndexError):
                continue

        if not daily_gmv:
            print("[Sheets] T열 일자별 요약: 유효한 데이터 없음")
            return None

        total = cumulative[-1] if cumulative else sum(daily_gmv)
        print(f"[Sheets] T열 일자별 요약: {len(daily_gmv)}일치 로드 (총 GMV: {total:,}원)")
        return {
            "daily_gmv": daily_gmv,
            "cumulative": cumulative,
            "dates": dates,
            "total": total,
        }
    except Exception as e:
        print(f"[Sheets] T열 일자별 요약 로드 실패: {e}")
        return None


def load_full_brand_data():
    """3개 시트에서 브랜드 데이터를 읽어 dict로 반환.

    2월의 누적 일수를 기준으로 11월/12월도 동일 일수만큼만 가져와서
    공정한 동기간 비교가 되도록 함.
    예: 2월이 2일차(0223,0224) → 11월 2일(1117,1118), 12월 2일(1222,1223)

    Returns:
        {"nov": DataFrame, "dec": DataFrame, "feb": DataFrame} 또는 실패 시 None
    """
    try:
        from googleapiclient.discovery import build

        creds = get_sheets_credentials()
        if creds is None:
            return None

        service = build("sheets", "v4", credentials=creds)
        print("[Sheets] Google Sheets API 연결 성공")

        # 1) 각 시트의 날짜 목록 조회
        feb_dates = get_sheet_dates(SHEET_NAMES["feb"], service)
        nov_dates = get_sheet_dates(SHEET_NAMES["nov"], service)
        dec_dates = get_sheet_dates(SHEET_NAMES["dec"], service)

        # 2) 2월 누적 일수 기준으로 동일 일수 적용
        num_days = len(feb_dates)  # 2월의 현재 누적 일수
        print(f"[Sheets] 2월 누적 {num_days}일치 기준으로 동기간 비교")

        # 각 행사의 처음 N일 날짜만 사용
        feb_allowed = feb_dates[:num_days]        # 2월: 전체 (기준)
        nov_allowed = nov_dates[:num_days]        # 11월: 처음 N일
        dec_allowed = dec_dates[:num_days]        # 12월: 처음 N일

        # 3) 동일 일수 기준으로 데이터 로드
        brand_dfs = {}
        brand_dfs["nov"] = fetch_sheet_brand_data(SHEET_NAMES["nov"], service, allowed_dates=nov_allowed)
        brand_dfs["dec"] = fetch_sheet_brand_data(SHEET_NAMES["dec"], service, allowed_dates=dec_allowed)
        brand_dfs["feb"] = fetch_sheet_brand_data(SHEET_NAMES["feb"], service, allowed_dates=feb_allowed)

        # 모든 시트에서 데이터를 가져왔는지 확인
        total = sum(len(df) for df in brand_dfs.values())
        if total == 0:
            print("[Sheets] 경고: 모든 시트에서 데이터를 읽지 못했습니다")
            return None

        print(f"[Sheets] 전체 {total}개 상품 데이터 로드 완료 (동기간 {num_days}일 비교)")
        return brand_dfs

    except Exception as e:
        print(f"[Sheets] 전체 데이터 로드 실패: {e}")
        print("[Sheets] → 기존 하드코딩 데이터로 fallback합니다")
        return None


# =============================================================================
# 1단계: 데이터 정의 (Google Sheets에서 수집한 데이터)
# =============================================================================

# --- 11월 블랙위크 상품 데이터 ---
nov_data_raw = [
    ["1117","패션/잡화","1001617821","[R2W] 25F/W 캐시미어 100 베이직 라운드 니트 9color (택1)","R2W","1p",677,"132,827,400"],
    ["1117","패션/잡화","1001617972","[R2W] 25F/W 캐시미어 100 베이직 브이 니트 7color (택1)","R2W","1p",483,"103,458,600"],
    ["1117","패션/잡화","1001618036","[R2W] 캐시미어 100 와이드 솔리드 머플러 6color (택1)","R2W","1p",278,"64,551,600"],
    ["1117","패션/잡화","1001618053","[R2W] 킹슬리 레더 버클 패딩 점퍼 2color (택1)","R2W","1p",163,"62,787,600"],
    ["1117","패션/잡화","1001618042","[R2W] 리카 울 캐시 판초 핸드메이드 코트 2color (택1)","R2W","1p",47,"25,295,400"],
    ["1117","주방용품","1001068937","[락앤락] 바로한끼 안심 도자기 밥 보관 용기 (도자기 4p)","락앤락","3pl",789,"16,001,100"],
    ["1117","생활용품","5138446","[Kurly's] 데일리 물티슈100매 (10팩/20팩) 2종 (택1)","기타","1p",1114,"15,773,300"],
    ["1117","유아동","1001525537","[킨도](박스) 아기가 감탄하는 기저귀 킨도! 샛별배송","킨도","3pl",261,"15,095,450"],
    ["1117","생활용품","1000745338","[라엘] 순면 커버 생리대 7종 (택1)","라엘","3pl",2010,"13,593,000"],
    ["1117","생활용품","5074264","[KS365] 3겹 천연펄프 더 오래쓰는 화장지 (40m X 18롤)","KS365","1p",1051,"13,557,900"],
    ["1117","가전제품","1001599395","[스텐팟] 26년형 6L 대용량 스텐 가열식 가습기","스텐팟","3p",38,"13,262,000"],
    ["1117","유아동","1001543101","[이글루] 스마트 홈카메라 인기 구성 모음전","이글루","3pl",146,"12,314,000"],
    ["1117","생활용품","5074265","[KS365] 3겹 천연펄프 화장지 (27m X 30롤)","KS365","1p",974,"11,940,260"],
    ["1117","생활용품","1001579102","[Kurly's] 손에 묻어나지 않는 포켓용 손난로 핫팩 90g X 30개입","기타","1p",977,"9,672,300"],
    ["1117","가전제품","1001414763","[클래파] 완벽세척 2단 올스텐 계란찜기","클래파","3p",305,"9,370,500"],
    ["1117","주방용품","1000391713","[놋담] 방짜유기 꽃 샐러드볼","놋담","3p",131,"9,025,900"],
    ["1117","가구/인테리어","1001544370","[오리고] 플리스 블랭킷 5종 (택1)","오리고","3p",217,"7,948,800"],
    ["1117","가전제품","1001231098","[소니] 렌즈 SEL2870GM","소니","3p",2,"7,918,000"],
    ["1117","주방용품","1000422696","[놋담] 방짜유기 봄꽃찬기 1호 2호 2P SET","놋담","3p",113,"7,898,700"],
    ["1117","가전제품","1001231072","[소니] 렌즈 SEL2470GM2","소니","3p",3,"7,617,000"],
    ["1117","패션/잡화","1001765083","[liwuliwu] 덕다운 글로시 하프 패딩","liwuliwu","3p",24,"7,300,800"],
    ["1117","유아동","1001554386","[꼬메모이] 모이듀 원형 회전책장 4단","꼬메모이","3p",16,"6,720,000"],
    ["1117","패션/잡화","1001740634","[R2W] 멜로니아 울 맥시 스커트","R2W","3pl",38,"6,570,200"],
    ["1117","패션/잡화","1001742702","[R2W] 카리사 헤비 울 팬츠","R2W","3pl",25,"6,365,000"],
    ["1117","생활용품","1000745345","[라엘] 유기농 순면커버 입는 오버나이트 생리대 3종 (택1)","라엘","3pl",481,"6,259,800"],
    ["1117","패션/잡화","1001740635","[R2W] 헤비 율리아 롱 티","R2W","3pl",93,"6,007,800"],
    ["1117","생활용품","1001107552","[파세오] 4겹 프리미엄 디럭스 화장지 25M X 30롤","파세오","3pl",255,"5,737,500"],
    ["1117","패션/잡화","1001740637","[R2W] 리아코 벨벳 밴딩 팬츠","R2W","3pl",39,"5,483,400"],
    ["1117","유아동","1001630455","[하기스] new2025 네이처메이드 밴드형/팬티형 기저귀 10종 (택 1)","하기스","3pl",181,"5,391,900"],
    ["1117","가전제품","1001138263","[자일렉] 글라스 에어프라이어 4.5L","자일렉","3p",51,"5,051,300"],
]

# --- 12월 리빙페스타 상품 데이터 ---
dec_data_raw = [
    ["1222","생활용품","1001533332","[숨] 100% 천연펄프 무형광 프리미엄 블랙 화장지 3겹 30m 30롤 2팩","숨","3p",2066,"39,047,400"],
    ["1222","유아동","1001495255","[킨도] (박스) 아이가 감탄하는 기저귀 킨도! 베스트 상품모음","킨도","3p",381,"23,022,540"],
    ["1222","유아동","1001724969","[플레이송스] 조이핸들","플레이송스","3p",2043,"22,473,000"],
    ["1222","유아동","1001436117","[팸퍼스] 2025 엔젤브리즈 팬티형 기저귀 3팩+3팩","팸퍼스","3p",88,"9,866,560"],
    ["1222","유아동","1001630455","[하기스] new2025 네이처메이드 밴드형/팬티형 기저귀 10종 (택 1)","하기스","3pl",306,"8,321,400"],
    ["1222","가전제품","1001599395","[스텐팟] 26년형 6L 대용량 스텐 가열식 가습기","스텐팟","3p",21,"7,329,000"],
    ["1222","가전제품","1001708038","[미닉스] 더시프트 미니 김치냉장고 39L","미닉스","3p",18,"7,182,000"],
    ["1222","가전제품","1001138266","[자일렉] 광파 오븐형 에어프라이어 18L","자일렉","3p",81,"6,654,700"],
    ["1222","가구/인테리어","1001544370","[오리고] 플리스 블랭킷 5종 (택1)","오리고","3p",207,"6,415,200"],
    ["1222","생활용품","1000867474","[테리파머] 200g 자카드 웨이브/스페셜/투페이스 호텔수건 모음전 (택1)","테리파머","3p",162,"5,821,800"],
    ["1222","생활용품","1000745338","[라엘] 순면 커버 생리대 7종 (택1)","라엘","3pl",836,"5,356,100"],
    ["1222","생활용품","1001107552","[파세오] 4겹 프리미엄 디럭스 화장지 25M X 30롤","파세오","3pl",228,"5,130,000"],
    ["1222","생활용품","1001631615","[크리넥스] 3겹 천연펄프 데코앤소프트 화장지 34m X 24롤","크리넥스","3pl",261,"4,739,900"],
    ["1222","가전제품","1001414763","[클래파] 완벽세척 2단 올스텐 계란찜기","클래파","3p",114,"3,948,600"],
    ["1222","생활용품","1000810869","[히트템] 포유 핫팩 90g (30입)","히트템","3pl",397,"3,568,700"],
    ["1222","유아동","1000858570","[팸퍼스] 2025 베이비드라이 팬티형 기저귀 4+4팩","팸퍼스","3p",25,"3,444,750"],
    ["1222","생활용품","1001579102","[Kurly's] 손에 묻어나지 않는 포켓용 손난로 핫팩 90g X 30개입","기타","1p",331,"3,276,900"],
    ["1222","가구/인테리어","1001324284","[드리울] 모달 패드일체형 올인원 누빔 매트리스커버","드리울","3p",47,"3,233,300"],
    ["1222","가전제품","1001138263","[자일렉] 글라스 에어프라이어 4.5L","자일렉","3p",31,"3,082,900"],
    ["1222","생활용품","1000632818","[매직캔] 휴지통 히포2크롬 오토실링 21L/27L","매직캔","3p",54,"2,955,800"],
    ["1222","가전제품","1000000329","[드롱기] 마그니피카 에보 전자동 커피머신","드롱기","3p",4,"2,796,000"],
    ["1222","생활용품","1000861713","[프로쉬] 식기세척기 주방세제 그린레몬 미니 50개입","프로쉬","3pl",173,"2,750,700"],
    ["1222","주방용품","1001548407","[트루쿡] 국내생산 후라이팬 웍 냄비 계란말이팬 17종","트루쿡","3pl",64,"2,589,000"],
    ["1222","유아동","1001096187","[하기스] 팬티&밴드 기저귀 1팩단위 BEST 모음전 (택 1)","하기스","3pl",110,"2,569,720"],
    ["1222","가전제품","1001336369","[발뮤다] NEW 팟 KPT01KR 전기주전자 3종","발뮤다","3p",16,"2,520,000"],
    ["1222","주방용품","1000785514","[프로그] 고무장갑 컴포트 퍼플 3개입 & 5개입 6종 (택1)","프로그","3pl",413,"2,324,100"],
    ["1222","가구/인테리어","1000796932","[드리울] 진드기차단 완벽 방수 매트리스커버","드리울","3p",118,"2,299,500"],
    ["1222","유아동","1001525537","[킨도](박스) 아기가 감탄하는 기저귀 킨도! 샛별배송","킨도","3pl",38,"2,287,870"],
    ["1222","가구/인테리어","1001730515","[아망떼] 소울메이트 장모 극세사 침대패드","아망떼","3p",63,"2,194,400"],
    ["1222","주방용품","1001476376","[알텐바흐] 엑스쿠첸 316Ti 통5중 저압냄비 3종 (택1)","알텐바흐","3p",11,"2,173,000"],
]

# --- 2월 리빙페스타 상품 데이터 ---
feb_data_raw = [
    ["0224","가전제품","1001728375","[미닉스] 음식물처리기 더플렌더MAX 3L","미닉스","3p",130,"68,949,000"],
    ["0224","유아동","1001631898","[베이비부스트] 올인원 이지 분유포트 5세대/4세대 (택1)","베이비부스트","3p",88,"15,556,000"],
    ["0224","생활용품","5138446","[Kurly's] 데일리 물티슈100매 (10팩/20팩) 2종 (택1)","기타","1p",1299,"15,595,800"],
    ["0224","생활용품","5074264","[KS365] 3겹 천연펄프 더 오래쓰는 화장지 (40m X 18롤)","KS365","1p",1203,"14,112,600"],
    ["0224","생활용품","1001631615","[크리넥스] 3겹 천연펄프 데코앤소프트 화장지 34m X 24롤","크리넥스","3pl",608,"13,134,950"],
    ["0224","가전제품","1001492421","[LG전자] LG 트롬 세탁기 (F12WVA)","LG전자","3p",16,"10,800,000"],
    ["0224","가구/인테리어","1001252238","[아망떼] 60수면 피그먼트 타임리스 고정밴드 침대패드","아망떼","3p",253,"10,368,800"],
    ["0224","생활용품","5074265","[KS365] 3겹 천연펄프 화장지 (27m X 30롤)","KS365","1p",827,"9,391,200"],
    ["0224","가전제품","1001708038","[미닉스] 더시프트 미니 김치냉장고 39L","미닉스","3p",23,"9,725,000"],
    ["0224","가구/인테리어","1001324284","[드리울] 모달 패드일체형 올인원 누빔 매트리스커버","드리울","3p",144,"8,388,700"],
    ["0224","주방용품","1001068937","[락앤락] 바로한끼 안심 도자기 밥 보관 용기 (도자기 4p)","락앤락","3pl",411,"8,694,400"],
    ["0224","생활용품","1000861713","[프로쉬] 식기세척기 주방세제 그린레몬 미니 50개입","프로쉬","3pl",475,"7,393,500"],
    ["0224","가구/인테리어","1001810719","[템퍼] 오리지날 베개 9종 (택1)","템퍼","3p",50,"6,200,000"],
    ["0224","주방용품","1001451355","[휘슬러] 비타빗 프리미엄 압력솥 S4 5종 (택1)","휘슬러","3p",12,"6,005,000"],
    ["0224","주방용품","1001793704","[트루쿡] 긁힘 없는 TPU 도마 세트","트루쿡","3pl",242,"5,971,100"],
    ["0224","유아동","1001630455","[하기스] new2025 네이처메이드 밴드형/팬티형 기저귀 10종 (택 1)","하기스","3pl",219,"4,274,100"],
    ["0224","생활용품","1000745338","[라엘] 순면 커버 생리대 7종 (택1)","라엘","3pl",829,"5,403,000"],
    ["0224","유아동","1001868718","[금아당] 24K 순금 돌반지","금아당","3p",5,"5,541,600"],
    ["0224","가전제품","1001414763","[클래파] 완벽세척 2단 올스텐 계란찜기","클래파","3p",136,"5,012,000"],
    ["0224","유아동","1001728245","[하기스] new 2025 팬티&밴드 기저귀 1팩단위 BEST모음 (택1)","하기스","3pl",164,"3,548,550"],
    ["0224","생활용품","5074267","[KS365] 2겹 천연펄프 키친타월 (130매 X 6롤)","KF365","1p",877,"4,019,600"],
    ["0224","생활용품","5132839","[KS365] 물티슈 엠보싱 캡형 100매","KS365","1p",323,"3,815,100"],
    ["0224","가전제품","1001138266","[자일렉] 광파 오븐형 에어프라이어 18L","자일렉","3p",49,"3,804,200"],
    ["0224","생활용품","1000929808","[액츠] 더블케어 플러스 세탁세제 4종 (택1)","액츠","3pl",408,"3,435,700"],
    ["0224","주방용품","1000785514","[프로그] 고무장갑 컴포트 퍼플 3개입 & 5개입 6종 (택1)","프로그","3pl",740,"3,800,600"],
    ["0224","가구/인테리어","1001924283","[더 조선호텔] 프리미엄 타월 3/5/10P SET","더 조선호텔","3pl",59,"3,276,000"],
    ["0224","유아동","1001495255","[킨도] (박스) 아이가 감탄하는 기저귀 킨도! 베스트 상품모음","킨도","3p",60,"2,855,700"],
    ["0224","가구/인테리어","1000796932","[드리울] 진드기차단 완벽 방수 매트리스커버","드리울","3p",196,"3,331,300"],
    ["0224","주방용품","1001548407","[트루쿡] 국내생산 후라이팬 웍 냄비 계란말이팬 17종","트루쿡","3pl",79,"3,271,500"],
    ["0224","주방용품","1001542860","[스타우브] 베이비웍 16cm 3종 (택1)","스타우브","3p",24,"3,640,000"],
]


def parse_gmv(val):
    """문자열 형태의 금액을 숫자로 변환 (예: '132,827,400' → 132827400)"""
    if isinstance(val, (int, float)):
        return int(val)
    return int(str(val).replace(",", "").replace(" ", ""))


def make_df(raw_data):
    """raw 데이터 리스트를 pandas DataFrame으로 변환"""
    rows = []
    for r in raw_data:
        rows.append({
            "ord_dt": r[0],
            "대카테고리": r[1],
            "콘텐츠코드": r[2],
            "콘텐츠 상품명": r[3],
            "브랜드명": r[4],
            "소싱유형": r[5],
            "판매수량": int(r[6]),
            "gmv2": parse_gmv(r[7]),
        })
    return pd.DataFrame(rows)


# DataFrame 생성
df_nov = make_df(nov_data_raw)
df_dec = make_df(dec_data_raw)
df_feb = make_df(feb_data_raw)

# =============================================================================
# 2단계: 요약 데이터 (스프레드시트에서 확인한 카테고리별/소싱유형별 합산)
# =============================================================================

# --- 카테고리별 GMV2 (전체 기간, 시트 요약 데이터 기준) ---
categories = ["가구/인테리어", "가전제품", "반려동물", "생활용품", "스포츠/레저",
              "여행/문화/서비스", "유아동", "주방용품", "패션/잡화"]

# 11월 전체(15일) 카테고리별 (시트 요약 데이터)
nov_cat_gmv = {
    "가구/인테리어": 1546434160,
    "가전제품": 2396261200,
    "반려동물": 466998720,
    "생활용품": 3081807030,
    "스포츠/레저": 45421650,
    "여행/문화/서비스": 3586000,
    "유아동": 1051762395,
    "주방용품": 2681047845,
    "패션/잡화": 1868832304,
}

# 12월 전체(10일) - 시트의 "데일리" 요약이 아닌, 전체 합계 사용 (5,430,191,680원)
# 시트에 카테고리별 데이터 확인 결과 (데일리 기준 수치가 첫날 것으로 보임, 전체 합산 사용)
dec_cat_gmv = {
    "가구/인테리어": 378905230,
    "가전제품": 444298290,
    "반려동물": 155426280,
    "생활용품": 1675476320,
    "스포츠/레저": 17510050,
    "여행/문화/서비스": 2868140,
    "유아동": 959268960,
    "주방용품": 687174980,
    "패션/잡화": 1109263430,
}

# 12월 전체 합산 정정 - 실제 시트 합계는 5,430,191,680
# 비율 기준으로 재계산 (데일리 기준 비율 사용)
dec_daily_cat = {
    "가구/인테리어": 70727510,
    "가전제품": 71303290,
    "반려동물": 28124980,
    "생활용품": 188614870,
    "스포츠/레저": 3162250,
    "여행/문화/서비스": 517140,
    "유아동": 131493990,
    "주방용품": 122003070,
    "패션/잡화": 182630,
}
dec_daily_total = sum(dec_daily_cat.values())
dec_total_gmv = 5430191680
# 전체 기간 카테고리별 = 비율 기준 추정
for k in dec_daily_cat:
    dec_cat_gmv[k] = int(dec_daily_cat[k] / dec_daily_total * dec_total_gmv) if dec_daily_total > 0 else 0

# 2월 (2일, D+1, D+2) 카테고리별
feb_cat_gmv = {
    "가구/인테리어": 93256570,
    "가전제품": 142351100,
    "반려동물": 28030190,
    "생활용품": 286536000,
    "스포츠/레저": 1568300,
    "여행/문화/서비스": 309000,
    "유아동": 61669850,
    "주방용품": 164127580,
    "패션/잡화": 11017680,
}

# --- 소싱유형별 GMV2 ---
# 11월 전체 (시트 소싱유형별)
nov_sourcing = {"1p": 1041335160, "3p": 5225890520, "3pl": 2664669590}
# 12월 첫날 기준 데일리: 1p: 8,760,330 / 3p: 227,887,710 / 3pl: 184,273,150
# 12월 전체는 비율로 추정
dec_sourcing_daily = {"1p": 8760330, "3p": 227887710, "3pl": 184273150}
dec_s_total = sum(dec_sourcing_daily.values())
dec_sourcing = {k: int(v / dec_s_total * dec_total_gmv) for k, v in dec_sourcing_daily.items()}
# 2월 (시트 요약)
feb_sourcing = {"1p": 95249290, "3p": 392308640, "3pl": 301308340}

# --- 일자별 누적 GMV 데이터 (D-day 기준) ---
# 11월 블랙위크: 시작일 11/17, 15일 진행
nov_daily_gmv = [
    836935430, 826220620, 835934785, 817999970, 735335130,
    731281010, 808617440, 910178610, 865498685, 789630260,
    774263330, 727371840, 713837260, 900214630, 925985835,
]

# 12월 리빙페스타: 시작일 12/22, 10일 진행
dec_daily_gmv = [
    615947100, 827297180, 519022430, 557015950, 542664880,
    536933880, 647316400, 618667410, 565326450, 420486310,
]

# 2월 리빙페스타: 시작일 2/23, 2일 진행 (현재)
feb_daily_gmv = [
    788866270,  # D+1 (2/23)
    # D+2는 아직 데이터 없음 (진행중이므로 총합 - D1으로 계산)
]

# 총합이 788,866,270이므로 D+1만 존재
feb_total = 788866270

# =============================================================================
# 3단계: 총 KPI 계산
# =============================================================================

# 11월 전체
nov_total_gmv = sum(nov_daily_gmv)  # 12,199,304,835
nov_total_qty = 28695  # 상품 데이터에서 합산 추정
nov_product_count = 203
nov_period = "11/17 ~ 12/1 (15일)"

# 12월 전체
dec_total_gmv_val = sum(dec_daily_gmv)  # 5,850,677,990
dec_total_qty = 15580
dec_product_count = 193
dec_period = "12/22 ~ 12/31 (10일)"

# 2월 (현재 진행중)
feb_total_gmv_val = feb_total
feb_total_qty = sum(df_feb["판매수량"])
feb_product_count = len(df_feb)
feb_period = "2/23 ~ 진행중 (1일차)"

# 실제 KPI용 총 판매수량 재계산 (전체 시트 기준)
# 11월 총GMV: 시트 내 소싱유형 합계 = 8,931,895,270 (총계는 실제 13,142,151,304이지만 이는 전체 포함)
# 시트의 총계 사용
nov_total_gmv = 12199304835  # 누적 GMV 합계
dec_total_gmv_val = 5850677990
feb_total_gmv_val = 788866270

# =============================================================================
# 4단계: Plotly 차트 생성
# =============================================================================

def generate_sparkline_svg(data, color="#5f0080", width=120, height=36):
    """일별 GMV 데이터를 SVG 스파크라인으로 변환
    - data: 숫자 리스트 (일별 GMV)
    - color: 라인 색상
    - width, height: SVG 크기
    - 반환값: SVG 문자열 (HTML에 직접 삽입 가능)
    """
    if not data or len(data) < 1:
        return ""

    # 데이터 정규화 (0~height 범위로 매핑, 여백 포함)
    padding = 4
    min_val = min(data)
    max_val = max(data)
    val_range = max_val - min_val if max_val != min_val else 1

    points = []
    for i, val in enumerate(data):
        x = padding + (i / max(len(data) - 1, 1)) * (width - padding * 2)
        y = height - padding - ((val - min_val) / val_range) * (height - padding * 2)
        points.append((x, y))

    # SVG path (라인)
    path_d = f"M {points[0][0]:.1f},{points[0][1]:.1f}"
    for x, y in points[1:]:
        path_d += f" L {x:.1f},{y:.1f}"

    # 영역 채우기용 path (라인 + 하단 닫기)
    fill_d = path_d + f" L {points[-1][0]:.1f},{height} L {points[0][0]:.1f},{height} Z"

    # 마지막 포인트 강조 원
    last_x, last_y = points[-1]

    # hex color를 rgb로 변환 (fillcolor용)
    hex_color = color.lstrip('#')
    r, g, b = int(hex_color[0:2], 16), int(hex_color[2:4], 16), int(hex_color[4:6], 16)

    svg = f'''<svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg">
        <path d="{fill_d}" fill="rgba({r},{g},{b},0.12)" />
        <path d="{path_d}" fill="none" stroke="{color}" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <circle cx="{last_x:.1f}" cy="{last_y:.1f}" r="3" fill="{color}" />
    </svg>'''
    return svg


def format_krw(val):
    """금액을 읽기 쉬운 한국어 형태로 변환
    - 1억 이상: X.X억
    - 1000만 이상: X,XXX만
    - 100만 이상: XXX만
    - 그 외: 콤마 표기
    """
    if val >= 100_000_000:  # 1억 이상
        return f"{val/100_000_000:.1f}억"
    elif val >= 1_000_000:  # 100만 이상
        return f"{val/10_000:,.0f}만"
    else:
        return f"{val:,.0f}"


def format_krw_full(val):
    """금액을 억 단위로 표시"""
    if val >= 100_000_000:
        return f"{val/100_000_000:.1f}억원"
    elif val >= 10_000:
        return f"{val/10_000:,.0f}만원"
    else:
        return f"{val:,.0f}원"

# Plotly 공통 레이아웃 스타일 (배경 투명, 그리드 미묘, 폰트 통일)
CHART_LAYOUT = dict(
    paper_bgcolor='rgba(0,0,0,0)',
    plot_bgcolor='rgba(0,0,0,0)',
    font=dict(family="Pretendard, -apple-system, sans-serif"),
    margin=dict(l=50, r=30, t=60, b=40),  # 통일된 기본 margin (오른쪽 공백 제거)
    autosize=True,  # 컨테이너 너비에 맞춰 자동 확장
)
CHART_GRID = dict(gridcolor='rgba(0,0,0,0.06)', zerolinecolor='rgba(0,0,0,0.08)')

# 색상 테마 (컬리 보라색 기반 + 명확한 대비)
COLORS = {
    "11월": "#5f0080",   # 컬리 딥 퍼플
    "12월": "#F57C00",   # 앰버/오렌지 (보라와 명확한 대비)
    "2월": "#00BFA5",    # 밝은 틸 (더 선명하게 강조)
}
# 컬리 보라색 팔레트
KURLY_PURPLE = "#5f0080"
KURLY_LIGHT = "#9C27B0"
KURLY_BG = "#F3E5F5"

# --- 섹션 2: 동일자(D-day) 매출 추이 비교 ---
def create_daily_trend_chart():
    """D-day 기준 일별/누적 GMV2 라인 차트"""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("일별 GMV2 (D-day 기준)", "누적 GMV2 (D-day 기준)"),
        horizontal_spacing=0.12
    )

    # 일별 GMV (그리기 순서: 11월→12월→2월, 2월이 맨 위에 표시)
    # 범례 순서: legendrank로 2월→12월→11월
    max_days = max(len(nov_daily_gmv), len(dec_daily_gmv), len(feb_daily_gmv))
    d_days = [f"D+{i+1}" for i in range(max_days)]

    fig.add_trace(go.Scatter(
        x=d_days[:len(nov_daily_gmv)], y=nov_daily_gmv,
        name="11월 블랙위크", line=dict(color=COLORS["11월"], width=2.5),
        mode="lines+markers", marker=dict(size=6),
        fill='tozeroy', fillcolor='rgba(95,0,128,0.08)',
        legendrank=3,
        hovertemplate="D+%{x}<br>GMV2: %{y:,.0f}원<extra>11월</extra>"
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=d_days[:len(dec_daily_gmv)], y=dec_daily_gmv,
        name="12월 리빙페스타", line=dict(color=COLORS["12월"], width=2.5),
        mode="lines+markers", marker=dict(size=6),
        fill='tozeroy', fillcolor='rgba(245,124,0,0.08)',
        legendrank=2,
        hovertemplate="D+%{x}<br>GMV2: %{y:,.0f}원<extra>12월</extra>"
    ), row=1, col=1)

    fig.add_trace(go.Scatter(
        x=d_days[:len(feb_daily_gmv)], y=feb_daily_gmv,
        name="2월 리빙페스타", line=dict(color=COLORS["2월"], width=3),
        mode="lines+markers", marker=dict(size=10),
        fill='tozeroy', fillcolor='rgba(0,191,165,0.12)',
        legendrank=1,
        hovertemplate="D+%{x}<br>GMV2: %{y:,.0f}원<extra>2월</extra>"
    ), row=1, col=1)

    # 11월/12월 평균 일매출 점선
    nov_avg = sum(nov_daily_gmv) / len(nov_daily_gmv)
    dec_avg = sum(dec_daily_gmv) / len(dec_daily_gmv)
    fig.add_hline(y=nov_avg, line_dash="dot", line_color=COLORS["11월"], opacity=0.5,
                  annotation_text=f"11월 {format_krw(nov_avg)}", annotation_position="top right",
                  annotation_font_size=9, annotation_font_color=COLORS["11월"],
                  row=1, col=1)
    fig.add_hline(y=dec_avg, line_dash="dot", line_color=COLORS["12월"], opacity=0.5,
                  annotation_text=f"12월 {format_krw(dec_avg)}", annotation_position="bottom right",
                  annotation_font_size=9, annotation_font_color=COLORS["12월"],
                  row=1, col=1)

    # 누적 GMV (그리기 순서: 11월→12월→2월)
    nov_cum = [sum(nov_daily_gmv[:i+1]) for i in range(len(nov_daily_gmv))]
    dec_cum = [sum(dec_daily_gmv[:i+1]) for i in range(len(dec_daily_gmv))]
    feb_cum = [sum(feb_daily_gmv[:i+1]) for i in range(len(feb_daily_gmv))]

    fig.add_trace(go.Scatter(
        x=d_days[:len(nov_cum)], y=nov_cum,
        name="11월 누적", line=dict(color=COLORS["11월"], width=2.5),
        mode="lines+markers", marker=dict(size=6), showlegend=False,
        hovertemplate="D+%{x}<br>누적 GMV2: %{y:,.0f}원<extra>11월</extra>"
    ), row=1, col=2)

    fig.add_trace(go.Scatter(
        x=d_days[:len(dec_cum)], y=dec_cum,
        name="12월 누적", line=dict(color=COLORS["12월"], width=2.5),
        mode="lines+markers", marker=dict(size=6), showlegend=False,
        hovertemplate="D+%{x}<br>누적 GMV2: %{y:,.0f}원<extra>12월</extra>"
    ), row=1, col=2)

    fig.add_trace(go.Scatter(
        x=d_days[:len(feb_cum)], y=feb_cum,
        name="2월 누적", line=dict(color=COLORS["2월"], width=3),
        mode="lines+markers", marker=dict(size=10), showlegend=False,
        hovertemplate="D+%{x}<br>누적 GMV2: %{y:,.0f}원<extra>2월</extra>"
    ), row=1, col=2)

    # D+1 동기간 비교 하이라이트 (수직선)
    for col in [1, 2]:
        fig.add_vline(x="D+1", line_width=1, line_dash="dash",
                     line_color="gray", opacity=0.5, row=1, col=col)

    fig.update_layout(
        height=420,
        legend=dict(orientation="h", yanchor="bottom", y=1.08, xanchor="center", x=0.5,
                    font=dict(size=11)),
        **CHART_LAYOUT,
    )
    fig.update_layout(margin=dict(l=40, r=20, t=60, b=40))
    # y축: 억 단위 표시로 겹침 방지
    fig.update_yaxes(
        tickvals=[i * 200_000_000 for i in range(7)],
        ticktext=[f"{i*2}억" for i in range(7)],
        **CHART_GRID, row=1, col=1
    )
    fig.update_yaxes(
        tickvals=[i * 2_000_000_000 for i in range(8)],
        ticktext=[f"{i*20}억" for i in range(8)],
        **CHART_GRID, row=1, col=2
    )
    fig.update_xaxes(range=[-0.5, max_days - 0.5], **CHART_GRID, row=1, col=1)
    fig.update_xaxes(range=[-0.5, max_days - 0.5], **CHART_GRID, row=1, col=2)

    return fig


# --- 섹션 3: 카테고리별 매출 비교 ---
def create_category_charts():
    """카테고리별 GMV2 그룹드 바 차트 + 구성비 스택드 바 차트"""
    fig = make_subplots(
        rows=1, cols=2,
        subplot_titles=("카테고리별 GMV2 비교", "카테고리별 GMV2 구성비 (%)"),
        horizontal_spacing=0.12
    )

    cats = categories
    nov_vals = [nov_cat_gmv.get(c, 0) for c in cats]
    dec_vals = [dec_cat_gmv.get(c, 0) for c in cats]
    feb_vals = [feb_cat_gmv.get(c, 0) for c in cats]

    # 그룹드 바 차트
    fig.add_trace(go.Bar(
        name="11월 블랙위크", x=cats, y=nov_vals,
        marker_color=COLORS["11월"], opacity=0.85,
        hovertemplate="%{x}<br>GMV2: %{y:,.0f}원<extra>11월</extra>"
    ), row=1, col=1)
    fig.add_trace(go.Bar(
        name="12월 리빙페스타", x=cats, y=dec_vals,
        marker_color=COLORS["12월"], opacity=0.85,
        hovertemplate="%{x}<br>GMV2: %{y:,.0f}원<extra>12월</extra>"
    ), row=1, col=1)
    fig.add_trace(go.Bar(
        name="2월 리빙페스타", x=cats, y=feb_vals,
        marker_color=COLORS["2월"], opacity=0.85,
        hovertemplate="%{x}<br>GMV2: %{y:,.0f}원<extra>2월</extra>"
    ), row=1, col=1)

    # 구성비 스택드 바
    nov_total = sum(nov_vals) or 1
    dec_total_c = sum(dec_vals) or 1
    feb_total_c = sum(feb_vals) or 1

    nov_pct = [v/nov_total*100 for v in nov_vals]
    dec_pct = [v/dec_total_c*100 for v in dec_vals]
    feb_pct = [v/feb_total_c*100 for v in feb_vals]

    months = ["11월 블랙위크", "12월 리빙페스타", "2월 리빙페스타"]
    cat_colors = px.colors.qualitative.Set3[:len(cats)]

    for i, cat in enumerate(cats):
        fig.add_trace(go.Bar(
            name=cat, x=months,
            y=[nov_pct[i], dec_pct[i], feb_pct[i]],
            marker_color=cat_colors[i],
            showlegend=True if i < len(cats) else False,
            legendgroup=cat,
            hovertemplate=f"{cat}<br>" + "%{y:.1f}%<extra></extra>"
        ), row=1, col=2)

    fig.update_layout(
        barmode="group", height=500,
        legend=dict(orientation="h", yanchor="bottom", y=-0.35, xanchor="center", x=0.5,
                   font=dict(size=10)),
        **CHART_LAYOUT,
    )
    fig.update_layout(margin=dict(l=50, r=30, t=60, b=120))
    # 두번째 차트는 stacked
    fig.update_layout(barmode="group")
    # 두번째 subplot만 stacked 적용하기 위해 별도 처리

    return fig


def create_category_stacked():
    """카테고리 구성비 스택드 바 차트 (별도)"""
    cats = categories
    nov_vals = [nov_cat_gmv.get(c, 0) for c in cats]
    dec_vals = [dec_cat_gmv.get(c, 0) for c in cats]
    feb_vals = [feb_cat_gmv.get(c, 0) for c in cats]

    nov_total = sum(nov_vals) or 1
    dec_total_c = sum(dec_vals) or 1
    feb_total_c = sum(feb_vals) or 1

    cat_colors = ["#5f0080", "#F57C00", "#00897B", "#E91E63", "#FFB300",
                  "#7B1FA2", "#26A69A", "#FF7043", "#5C6BC0"]

    fig = go.Figure()
    months = ["11월 블랙위크", "12월 리빙페스타", "2월 리빙페스타"]

    for i, cat in enumerate(cats):
        nov_pct = nov_vals[i]/nov_total*100
        dec_pct = dec_vals[i]/dec_total_c*100
        feb_pct = feb_vals[i]/feb_total_c*100
        fig.add_trace(go.Bar(
            name=cat, x=months,
            y=[nov_pct, dec_pct, feb_pct],
            marker_color=cat_colors[i],
            text=[f"{nov_pct:.1f}%" if nov_pct > 3 else "",
                  f"{dec_pct:.1f}%" if dec_pct > 3 else "",
                  f"{feb_pct:.1f}%" if feb_pct > 3 else ""],
            textposition="inside",
            hovertemplate=f"{cat}<br>" + "비중: %{y:.1f}%<br>" +
                         f"금액: " + "%{customdata:,.0f}원<extra></extra>",
            customdata=[nov_vals[i], dec_vals[i], feb_vals[i]]
        ))

    fig.update_layout(
        barmode="stack", height=400,
        yaxis_title="구성비 (%)",
        legend=dict(orientation="h", yanchor="bottom", y=-0.3, xanchor="center", x=0.5,
                   font=dict(size=10)),
        **CHART_LAYOUT,
    )
    fig.update_layout(margin=dict(l=50, r=30, t=30, b=100))
    fig.update_xaxes(**CHART_GRID)
    fig.update_yaxes(**CHART_GRID)
    return fig


def create_category_grouped():
    """카테고리별 GMV2 그룹드 바 차트"""
    cats = categories
    nov_vals = [nov_cat_gmv.get(c, 0) for c in cats]
    dec_vals = [dec_cat_gmv.get(c, 0) for c in cats]
    feb_vals = [feb_cat_gmv.get(c, 0) for c in cats]

    fig = go.Figure()
    fig.add_trace(go.Bar(
        name="2월 리빙페스타", x=cats, y=feb_vals,
        marker_color=COLORS["2월"], opacity=0.85,
        text=[format_krw(v) for v in feb_vals], textposition="outside",
        hovertemplate="%{x}<br>GMV2: %{y:,.0f}원<extra>2월</extra>"
    ))
    fig.add_trace(go.Bar(
        name="12월 리빙페스타", x=cats, y=dec_vals,
        marker_color=COLORS["12월"], opacity=0.85,
        text=[format_krw(v) for v in dec_vals], textposition="outside",
        hovertemplate="%{x}<br>GMV2: %{y:,.0f}원<extra>12월</extra>"
    ))
    fig.add_trace(go.Bar(
        name="11월 블랙위크", x=cats, y=nov_vals,
        marker_color=COLORS["11월"], opacity=0.85,
        text=[format_krw(v) for v in nov_vals], textposition="outside",
        hovertemplate="%{x}<br>GMV2: %{y:,.0f}원<extra>11월</extra>"
    ))

    fig.update_layout(
        barmode="group", height=450,
        yaxis_tickformat=",.0f",
        legend=dict(orientation="h", yanchor="bottom", y=1.05, xanchor="center", x=0.5),
        **CHART_LAYOUT,
    )
    fig.update_xaxes(**CHART_GRID)
    fig.update_yaxes(**CHART_GRID)
    return fig


# --- 섹션 4: 브랜드별 매출 비교 ---
def get_top_brands(df, n=10):
    """브랜드별 GMV2 합계 TOP n"""
    brand_gmv = df.groupby("브랜드명").agg(
        gmv2=("gmv2", "sum"),
        판매수량=("판매수량", "sum")
    ).sort_values("gmv2", ascending=False).head(n).reset_index()
    return brand_gmv


def create_brand_chart():
    """월별 TOP 10 브랜드 수평 바 차트 (탭 전환)"""
    nov_brands = get_top_brands(df_nov)
    dec_brands = get_top_brands(df_dec)
    feb_brands = get_top_brands(df_feb)

    fig = go.Figure()

    # 2월 (첫 번째 - 기본 표시)
    fig.add_trace(go.Bar(
        y=feb_brands["브랜드명"][::-1], x=feb_brands["gmv2"][::-1],
        orientation="h", name="2월 리빙페스타",
        marker_color=COLORS["2월"], visible=True,
        text=[format_krw(v) for v in feb_brands["gmv2"][::-1]],
        textposition="outside",
        hovertemplate="%{y}<br>GMV2: %{x:,.0f}원<br>판매수량: %{customdata:,d}개<extra></extra>",
        customdata=feb_brands["판매수량"][::-1]
    ))

    # 12월
    fig.add_trace(go.Bar(
        y=dec_brands["브랜드명"][::-1], x=dec_brands["gmv2"][::-1],
        orientation="h", name="12월 리빙페스타",
        marker_color=COLORS["12월"], visible=False,
        text=[format_krw(v) for v in dec_brands["gmv2"][::-1]],
        textposition="outside",
        hovertemplate="%{y}<br>GMV2: %{x:,.0f}원<br>판매수량: %{customdata:,d}개<extra></extra>",
        customdata=dec_brands["판매수량"][::-1]
    ))

    # 11월
    fig.add_trace(go.Bar(
        y=nov_brands["브랜드명"][::-1], x=nov_brands["gmv2"][::-1],
        orientation="h", name="11월 블랙위크",
        marker_color=COLORS["11월"], visible=False,
        text=[format_krw(v) for v in nov_brands["gmv2"][::-1]],
        textposition="outside",
        hovertemplate="%{y}<br>GMV2: %{x:,.0f}원<br>판매수량: %{customdata:,d}개<extra></extra>",
        customdata=nov_brands["판매수량"][::-1]
    ))

    fig.update_layout(
        height=480,
        xaxis_tickformat=",.0f",
        **CHART_LAYOUT,
        updatemenus=[dict(
            type="buttons",
            direction="right",
            x=0.5, y=1.18,
            xanchor="center",
            font=dict(size=11),
            buttons=[
                dict(label="  2월  ",
                     method="update",
                     args=[{"visible": [True, False, False]}]),
                dict(label="  12월  ",
                     method="update",
                     args=[{"visible": [False, True, False]}]),
                dict(label="  11월  ",
                     method="update",
                     args=[{"visible": [False, False, True]}]),
            ]
        )],
    )
    fig.update_layout(margin=dict(l=90, r=60, t=90, b=30))
    fig.update_xaxes(**CHART_GRID)
    fig.update_yaxes(**CHART_GRID)
    return fig


def get_common_brands_table(brand_dfs=None, d_label="D+1"):
    """3개 월 공통 브랜드 매출 비교 테이블 HTML.

    Args:
        brand_dfs: Google Sheets 전체 데이터. None이면 기존 하드코딩 데이터 사용.
        d_label: 동기간 라벨 (예: 'D+2')
    """
    if brand_dfs is not None:
        src_nov = brand_dfs["nov"]
        src_dec = brand_dfs["dec"]
        src_feb = brand_dfs["feb"]
    else:
        src_nov = df_nov
        src_dec = df_dec
        src_feb = df_feb

    nov_brand = src_nov.groupby("브랜드명").agg(gmv2=("gmv2","sum"), qty=("판매수량","sum")).reset_index()
    dec_brand = src_dec.groupby("브랜드명").agg(gmv2=("gmv2","sum"), qty=("판매수량","sum")).reset_index()
    feb_brand = src_feb.groupby("브랜드명").agg(gmv2=("gmv2","sum"), qty=("판매수량","sum")).reset_index()

    # 공통 브랜드 찾기
    common = set(nov_brand["브랜드명"]) & set(dec_brand["브랜드명"]) & set(feb_brand["브랜드명"])

    if not common:
        # 2개월 이상 공통
        common_2 = (set(nov_brand["브랜드명"]) & set(dec_brand["브랜드명"])) | \
                   (set(dec_brand["브랜드명"]) & set(feb_brand["브랜드명"])) | \
                   (set(nov_brand["브랜드명"]) & set(feb_brand["브랜드명"]))
        common = common_2

    rows = []
    for brand in common:
        n = nov_brand[nov_brand["브랜드명"]==brand]
        d = dec_brand[dec_brand["브랜드명"]==brand]
        f = feb_brand[feb_brand["브랜드명"]==brand]
        rows.append({
            "브랜드": brand,
            "11월 GMV2": int(n["gmv2"].sum()) if len(n) > 0 else 0,
            "11월 수량": int(n["qty"].sum()) if len(n) > 0 else 0,
            "12월 GMV2": int(d["gmv2"].sum()) if len(d) > 0 else 0,
            "12월 수량": int(d["qty"].sum()) if len(d) > 0 else 0,
            "2월 GMV2": int(f["gmv2"].sum()) if len(f) > 0 else 0,
            "2월 수량": int(f["qty"].sum()) if len(f) > 0 else 0,
        })

    df_common = pd.DataFrame(rows)
    df_common["총합 GMV2"] = df_common["11월 GMV2"] + df_common["12월 GMV2"] + df_common["2월 GMV2"]
    df_common = df_common.sort_values("총합 GMV2", ascending=False).head(15)

    # HTML 테이블 생성 (모바일 가로 스크롤 지원)
    html = '<div class="table-scroll">'
    html += '<table class="data-table"><thead><tr>'
    html += '<th>브랜드</th><th style="text-align:right;">11월 GMV2</th><th style="text-align:right;">11월 수량</th>'
    html += '<th style="text-align:right;">12월 GMV2</th><th style="text-align:right;">12월 수량</th>'
    html += '<th style="text-align:right;">2월 GMV2</th><th style="text-align:right;">2월 수량</th></tr></thead><tbody>'

    for _, row in df_common.iterrows():
        html += '<tr>'
        html += f'<td class="brand-name">{row["브랜드"]}</td>'
        for col in ["11월 GMV2", "11월 수량", "12월 GMV2", "12월 수량", "2월 GMV2", "2월 수량"]:
            val = row[col]
            if "GMV2" in col:
                html += f'<td class="num">{val:,.0f}</td>'
            else:
                html += f'<td class="num">{val:,d}</td>'
        html += '</tr>'
    html += '</tbody></table></div>'
    return html


def create_brand_analysis(brand_dfs=None, d_label="D+1"):
    """브랜드별 실적 비교 분석 HTML 생성.

    Args:
        brand_dfs: Google Sheets 전체 데이터 {"nov": df, "dec": df, "feb": df}.
                   None이면 기존 하드코딩 데이터(df_nov/df_dec/df_feb)를 사용.
        d_label: 동기간 라벨 (예: 'D+2')
    """
    # 데이터 소스 선택: 전체 Sheets 데이터 vs 기존 하드코딩 데이터
    if brand_dfs is not None:
        src_nov = brand_dfs["nov"]
        src_dec = brand_dfs["dec"]
        src_feb = brand_dfs["feb"]
    else:
        src_nov = df_nov
        src_dec = df_dec
        src_feb = df_feb

    # 월별 브랜드 집계
    nov_brand = src_nov.groupby("브랜드명").agg(gmv2=("gmv2","sum"), qty=("판매수량","sum")).reset_index()
    dec_brand = src_dec.groupby("브랜드명").agg(gmv2=("gmv2","sum"), qty=("판매수량","sum")).reset_index()
    feb_brand = src_feb.groupby("브랜드명").agg(gmv2=("gmv2","sum"), qty=("판매수량","sum")).reset_index()

    nov_set = set(nov_brand["브랜드명"])
    dec_set = set(dec_brand["브랜드명"])
    feb_set = set(feb_brand["브랜드명"])

    # 1. 3개월 공통 브랜드
    common_all = nov_set & dec_set & feb_set
    # 2. 2월 신규 브랜드 (11월, 12월에 없던)
    feb_new = feb_set - nov_set - dec_set
    # 3. 2월 + 12월 공통 (11월엔 없음)
    dec_feb_only = (dec_set & feb_set) - nov_set
    # 4. 월별 브랜드 수
    nov_count = len(nov_set)
    dec_count = len(dec_set)
    feb_count = len(feb_set)

    # 공통 브랜드 실적 비교 테이블 (GMV 순 정렬)
    common_rows = []
    for brand in common_all:
        n_gmv = int(nov_brand[nov_brand["브랜드명"]==brand]["gmv2"].sum())
        d_gmv = int(dec_brand[dec_brand["브랜드명"]==brand]["gmv2"].sum())
        f_gmv = int(feb_brand[feb_brand["브랜드명"]==brand]["gmv2"].sum())
        # 2월 vs 12월 증감률
        change_vs_dec = ((f_gmv - d_gmv) / d_gmv * 100) if d_gmv > 0 else 0
        common_rows.append({
            "brand": brand, "nov": n_gmv, "dec": d_gmv, "feb": f_gmv,
            "change": change_vs_dec, "total": n_gmv + d_gmv + f_gmv
        })
    common_rows.sort(key=lambda x: x["feb"], reverse=True)

    # 2월 신규 브랜드 리스트
    new_rows = []
    for brand in feb_new:
        f_gmv = int(feb_brand[feb_brand["브랜드명"]==brand]["gmv2"].sum())
        new_rows.append({"brand": brand, "gmv": f_gmv})
    new_rows.sort(key=lambda x: x["gmv"], reverse=True)

    # 증감 화살표 헬퍼
    def arrow_html(pct):
        if pct > 0:
            return f'<span style="color:#E53935;font-weight:600;">▲{pct:.1f}%</span>'
        elif pct < 0:
            return f'<span style="color:#1E88E5;font-weight:600;">▼{abs(pct):.1f}%</span>'
        return '<span style="color:#888;">-</span>'

    # HTML 조합
    html = ''


    # 공통 브랜드 실적 비교 테이블 (검색 + 정렬 + 페이지네이션)
    if common_rows:
        html += f'<h4 style="font-size:15px; font-weight:600; margin-bottom:12px; color:#333;">3개월 공통 브랜드 {d_label} 실적 비교</h4>'

        # 검색창 + 정렬 토글 컨트롤 바
        html += '<div class="brand-controls">'
        html += '<input type="text" id="brandSearchInput" class="brand-search" placeholder="브랜드명 검색..." oninput="brandSearch()">'
        html += '<div class="brand-sort-buttons">'
        html += '<button class="sort-btn active" id="sortByGmv" onclick="brandSort(\'gmv\')">2월 GMV순</button>'
        html += '<button class="sort-btn" id="sortByChange" onclick="brandSort(\'change\')">증감률순</button>'
        html += '</div>'
        html += '</div>'

        # 테이블 본문 — data-brand, data-feb, data-change 속성 부여
        html += '<div class="table-scroll">'
        html += '<table class="data-table" id="brandTable"><thead><tr>'
        html += '<th style="width:40px;">#</th><th>브랜드</th><th style="text-align:right;">2월 GMV2</th><th style="text-align:right;">12월 GMV2</th><th style="text-align:right;">11월 GMV2</th><th style="text-align:center;">2월 vs 12월</th></tr></thead><tbody id="brandTbody">'
        for idx, r in enumerate(common_rows):
            html += f'<tr data-brand="{r["brand"]}" data-feb="{r["feb"]}" data-change="{r["change"]:.2f}">'
            html += f'<td class="rank">{idx+1}</td>'
            html += f'<td class="brand-name">{r["brand"]}</td>'
            html += f'<td class="num">{r["feb"]:,.0f}</td>'
            html += f'<td class="num">{r["dec"]:,.0f}</td>'
            html += f'<td class="num">{r["nov"]:,.0f}</td>'
            html += f'<td style="text-align:center;">{arrow_html(r["change"])}</td></tr>'
        html += '</tbody></table></div>'

        # 페이지네이션 UI
        html += '<div class="brand-pagination" id="brandPagination"></div>'

        # 결과 카운트
        html += f'<div class="brand-result-count" id="brandResultCount">전체 {len(common_rows)}개 브랜드</div>'

    # 2월 신규 브랜드 (TOP 10 기본 표시 + 접기/펼치기)
    if new_rows:
        html += '<h4 style="font-size:15px; font-weight:600; margin:24px 0 12px; color:#333;">2월 신규 참여 브랜드</h4>'
        # TOP 10 항상 표시
        html += '<div id="newBrandsVisible" style="display:flex; flex-wrap:wrap; gap:8px;">'
        for i, r in enumerate(new_rows[:10]):
            html += f'<span class="new-brand-tag">'
            html += f'<b>{r["brand"]}</b> <span style="color:#888; margin-left:4px;">{format_krw(r["gmv"])}</span></span>'
        html += '</div>'
        # 나머지 (숨김)
        if len(new_rows) > 10:
            html += '<div id="newBrandsHidden" style="display:none; flex-wrap:wrap; gap:8px; margin-top:8px;">'
            for r in new_rows[10:]:
                html += f'<span class="new-brand-tag">'
                html += f'<b>{r["brand"]}</b> <span style="color:#888; margin-left:4px;">{format_krw(r["gmv"])}</span></span>'
            html += '</div>'
            html += f'<button class="toggle-new-brands-btn" id="toggleNewBrandsBtn" onclick="toggleNewBrands()">전체 {len(new_rows)}개 보기 ▼</button>'

    # --- 브랜드 분석 JavaScript ---
    new_brand_count = len(new_rows) if new_rows else 0
    html += f'''<script>
(function(){{
    var PAGE_SIZE = 20;
    var currentPage = 1;
    var currentSort = 'gmv';
    var allRows = [];
    var filteredRows = [];

    // 초기화: tbody에서 모든 행 수집
    var tbody = document.getElementById('brandTbody');
    if (!tbody) return;
    var trs = tbody.querySelectorAll('tr');
    trs.forEach(function(tr) {{
        allRows.push(tr);
    }});
    filteredRows = allRows.slice();

    // 검색 필터
    window.brandSearch = function() {{
        var query = document.getElementById('brandSearchInput').value.toLowerCase().trim();
        filteredRows = allRows.filter(function(tr) {{
            return tr.getAttribute('data-brand').toLowerCase().indexOf(query) !== -1;
        }});
        // 현재 정렬 유지
        sortRows(currentSort);
        currentPage = 1;
        renderPage();
    }};

    // 정렬
    function sortRows(mode) {{
        if (mode === 'gmv') {{
            filteredRows.sort(function(a, b) {{
                return parseFloat(b.getAttribute('data-feb')) - parseFloat(a.getAttribute('data-feb'));
            }});
        }} else {{
            filteredRows.sort(function(a, b) {{
                return parseFloat(b.getAttribute('data-change')) - parseFloat(a.getAttribute('data-change'));
            }});
        }}
    }}

    window.brandSort = function(mode) {{
        currentSort = mode;
        document.getElementById('sortByGmv').classList.toggle('active', mode === 'gmv');
        document.getElementById('sortByChange').classList.toggle('active', mode === 'change');
        sortRows(mode);
        currentPage = 1;
        renderPage();
    }};

    // 페이지 이동
    window.brandPage = function(n) {{
        var totalPages = Math.ceil(filteredRows.length / PAGE_SIZE) || 1;
        if (n < 1 || n > totalPages) return;
        currentPage = n;
        renderPage();
    }};

    // 렌더링
    function renderPage() {{
        var totalPages = Math.ceil(filteredRows.length / PAGE_SIZE) || 1;
        if (currentPage > totalPages) currentPage = totalPages;
        var start = (currentPage - 1) * PAGE_SIZE;
        var end = start + PAGE_SIZE;

        // 모든 행 숨기기
        allRows.forEach(function(tr) {{ tr.style.display = 'none'; }});

        // 현재 페이지 행만 표시 + 순번 업데이트
        filteredRows.forEach(function(tr, i) {{
            if (i >= start && i < end) {{
                tr.style.display = '';
                tr.children[0].textContent = (i + 1);
            }}
        }});

        // 결과 카운트
        var countEl = document.getElementById('brandResultCount');
        if (countEl) {{
            var query = document.getElementById('brandSearchInput').value.trim();
            if (query) {{
                countEl.textContent = '검색 결과: ' + filteredRows.length + '개 / 전체 ' + allRows.length + '개';
            }} else {{
                countEl.textContent = '전체 ' + allRows.length + '개 브랜드';
            }}
        }}

        // 페이지네이션 버튼 생성
        var pagDiv = document.getElementById('brandPagination');
        if (!pagDiv) return;
        pagDiv.innerHTML = '';
        if (totalPages <= 1) return;

        // 이전 버튼
        var prevBtn = document.createElement('button');
        prevBtn.className = 'page-btn' + (currentPage === 1 ? ' disabled' : '');
        prevBtn.textContent = '\\u2190 이전';
        prevBtn.onclick = function() {{ brandPage(currentPage - 1); }};
        pagDiv.appendChild(prevBtn);

        // 페이지 번호 (최대 7개 표시)
        var startP = Math.max(1, currentPage - 3);
        var endP = Math.min(totalPages, startP + 6);
        if (endP - startP < 6) startP = Math.max(1, endP - 6);

        for (var p = startP; p <= endP; p++) {{
            var btn = document.createElement('button');
            btn.className = 'page-btn' + (p === currentPage ? ' active' : '');
            btn.textContent = p;
            btn.onclick = (function(page) {{ return function() {{ brandPage(page); }}; }})(p);
            pagDiv.appendChild(btn);
        }}

        // 다음 버튼
        var nextBtn = document.createElement('button');
        nextBtn.className = 'page-btn' + (currentPage === totalPages ? ' disabled' : '');
        nextBtn.textContent = '다음 \\u2192';
        nextBtn.onclick = function() {{ brandPage(currentPage + 1); }};
        pagDiv.appendChild(nextBtn);
    }}

    // 신규 브랜드 토글
    window.toggleNewBrands = function() {{
        var hidden = document.getElementById('newBrandsHidden');
        var btn = document.getElementById('toggleNewBrandsBtn');
        if (!hidden || !btn) return;
        if (hidden.style.display === 'none') {{
            hidden.style.display = 'flex';
            btn.textContent = '접기 \\u25B2';
        }} else {{
            hidden.style.display = 'none';
            btn.textContent = '전체 {new_brand_count}개 보기 \\u25BC';
        }}
    }};

    // 초기 렌더링
    renderPage();
}})();
</script>'''

    return html


# --- 섹션 5: 소싱유형별 비교 ---
def create_sourcing_charts():
    """소싱유형별 도넛 차트 3개"""
    fig = make_subplots(
        rows=1, cols=3,
        specs=[[{"type": "pie"}, {"type": "pie"}, {"type": "pie"}]],
        subplot_titles=("2월 리빙페스타", "12월 리빙페스타", "11월 블랙위크")
    )

    labels = ["1p", "3p", "3pl"]
    pie_colors = ["#5f0080", "#F57C00", "#00897B"]

    # 2월
    feb_vals = [feb_sourcing[k] for k in labels]
    fig.add_trace(go.Pie(
        labels=labels, values=feb_vals, hole=0.45,
        marker_colors=pie_colors,
        textinfo="percent",
        textposition="inside",
        textfont=dict(size=12, color="white"),
        hovertemplate="%{label}<br>GMV2: %{value:,.0f}원<br>비중: %{percent}<extra>2월</extra>"
    ), row=1, col=1)

    # 12월
    dec_vals = [dec_sourcing[k] for k in labels]
    fig.add_trace(go.Pie(
        labels=labels, values=dec_vals, hole=0.45,
        marker_colors=pie_colors,
        textinfo="percent",
        textposition="inside",
        textfont=dict(size=12, color="white"),
        hovertemplate="%{label}<br>GMV2: %{value:,.0f}원<br>비중: %{percent}<extra>12월</extra>"
    ), row=1, col=2)

    # 11월
    nov_vals = [nov_sourcing[k] for k in labels]
    fig.add_trace(go.Pie(
        labels=labels, values=nov_vals, hole=0.45,
        marker_colors=pie_colors,
        textinfo="percent",
        textposition="inside",
        textfont=dict(size=12, color="white"),
        hovertemplate="%{label}<br>GMV2: %{value:,.0f}원<br>비중: %{percent}<extra>11월</extra>"
    ), row=1, col=3)

    # 도넛 중앙에 총액 annotation 추가
    feb_total_s = sum(feb_vals)
    dec_total_s = sum(dec_vals)
    nov_total_s = sum(nov_vals)

    totals = [feb_total_s, dec_total_s, nov_total_s]

    # 각 도넛 중앙에 총액 표시 (domain 기반 좌표 계산)
    # subplot_titles가 상단 약 7% 공간을 차지하므로 y를 보정
    domains_x = [(0.0, 0.289), (0.356, 0.644), (0.711, 1.0)]
    for i, total in enumerate(totals):
        cx = (domains_x[i][0] + domains_x[i][1]) / 2
        cy = 0.47  # 상단 타이틀 고려한 도넛 실제 수직 중심
        fig.add_annotation(
            x=cx, y=cy, xref="paper", yref="paper",
            xanchor="center", yanchor="middle",
            text=f"<b>{format_krw(total)}</b>",
            showarrow=False, font=dict(size=13, color="#333", family="Poppins, Pretendard, sans-serif"),
        )

    fig.update_layout(
        height=350,
        showlegend=True,
        legend=dict(orientation="h", yanchor="top", y=-0.15, xanchor="center", x=0.5,
                    font=dict(size=14)),
        **CHART_LAYOUT,
    )
    fig.update_layout(margin=dict(l=10, r=10, t=50, b=70))
    return fig


# --- 섹션 6: TOP 상품 비교 테이블 ---
def create_top_products_table(df, month_name, n=15):
    """월별 TOP 15 상품 테이블 HTML (GMV2 셀에 프로그레스 바 포함)"""
    df_sorted = df.sort_values("gmv2", ascending=False).head(n).reset_index(drop=True)
    max_gmv = df_sorted["gmv2"].max() if len(df_sorted) > 0 else 1

    html = f'<div class="tab-content" id="tab-{month_name}">'
    html += '<table class="data-table"><thead><tr>'
    html += '<th>순위</th><th>상품명</th><th>브랜드</th><th>카테고리</th>'
    html += '<th>소싱유형</th><th>판매수량</th><th>GMV2</th></tr></thead><tbody>'

    for i, row in df_sorted.iterrows():
        rank = i + 1
        name = row["콘텐츠 상품명"]
        if len(name) > 40:
            name = name[:40] + "..."
        bar_pct = (row["gmv2"] / max_gmv * 100) if max_gmv > 0 else 0
        html += f'<tr><td class="rank">{rank}</td>'
        html += f'<td class="product-name" title="{row["콘텐츠 상품명"]}">{name}</td>'
        html += f'<td>{row["브랜드명"]}</td>'
        html += f'<td>{row["대카테고리"]}</td>'
        html += f'<td class="sourcing-{row["소싱유형"]}">{row["소싱유형"]}</td>'
        html += f'<td class="num">{row["판매수량"]:,d}</td>'
        html += f'<td class="gmv-bar"><span class="bar-bg" style="width:{bar_pct:.1f}%"></span><span class="bar-text">{row["gmv2"]:,.0f}</span></td></tr>'
    html += '</tbody></table></div>'
    return html


# --- 동기간 비교 KPI (D+1 기준) ---
def get_same_period_comparison():
    """D+1 기준 동기간 비교 데이터"""
    nov_d1 = nov_daily_gmv[0]  # 836,935,430
    dec_d1 = dec_daily_gmv[0]  # 615,947,100
    feb_d1 = feb_daily_gmv[0]  # 788,866,270

    return {
        "nov_d1": nov_d1,
        "dec_d1": dec_d1,
        "feb_d1": feb_d1,
        "feb_vs_nov": (feb_d1 - nov_d1) / nov_d1 * 100 if nov_d1 else 0,
        "feb_vs_dec": (feb_d1 - dec_d1) / dec_d1 * 100 if dec_d1 else 0,
    }


# =============================================================================
# 5단계: HTML 대시보드 조합
# =============================================================================

def build_html():
    """전체 HTML 대시보드 생성"""

    # Google Sheets 전체 브랜드 데이터 로드 (실패 시 None → fallback)
    brand_dfs = load_full_brand_data()

    # Google Sheets 데이터가 있으면 소싱유형별 GMV와 총 GMV를 동적으로 계산
    global nov_sourcing, dec_sourcing, feb_sourcing
    global nov_total_gmv, dec_total_gmv_val, feb_total_gmv_val, feb_period
    if brand_dfs is not None:
        for key, var_name in [("nov", "nov_sourcing"), ("dec", "dec_sourcing"), ("feb", "feb_sourcing")]:
            df = brand_dfs[key]
            sourcing_gmv = df.groupby("소싱유형")["gmv2"].sum().to_dict()
            # 1p, 3p, 3pl 키가 없으면 0으로 채움
            result = {"1p": sourcing_gmv.get("1p", 0), "3p": sourcing_gmv.get("3p", 0), "3pl": sourcing_gmv.get("3pl", 0)}
            if var_name == "nov_sourcing":
                nov_sourcing = result
            elif var_name == "dec_sourcing":
                dec_sourcing = result
            else:
                feb_sourcing = result

        # 동기간 비교용 GMV (D+N 기준)
        nov_same_period_gmv = brand_dfs["nov"]["gmv2"].sum()
        dec_same_period_gmv = brand_dfs["dec"]["gmv2"].sum()
        feb_total_gmv_val = brand_dfs["feb"]["gmv2"].sum()
        # 행사 개요 KPI: 11월/12월은 전체 기간 유지, 2월만 동적 업데이트
        # (nov_total_gmv, dec_total_gmv_val은 하드코딩 전체 기간 값 그대로 유지)

        # 2월 기간 표시 업데이트 (누적 일수 반영)
        num_days = len(set(brand_dfs["feb"].get("ord_dt", []))) if "ord_dt" in brand_dfs["feb"].columns else 0
        if num_days == 0:
            # ord_dt 컬럼이 없으면 Google Sheets에서 직접 확인
            from googleapiclient.discovery import build
            creds = get_sheets_credentials()
            if creds:
                svc = build("sheets", "v4", credentials=creds)
                feb_dates = get_sheet_dates(SHEET_NAMES["feb"], svc)
                num_days = len(feb_dates)
        if num_days > 0:
            feb_period = f"2/23 ~ 진행중 ({num_days}일차)"

        # 카테고리별 GMV 동적 계산
        global nov_cat_gmv, dec_cat_gmv, feb_cat_gmv
        for key, var_name in [("nov", "nov_cat_gmv"), ("dec", "dec_cat_gmv"), ("feb", "feb_cat_gmv")]:
            cat_data = brand_dfs[key].groupby("대카테고리")["gmv2"].sum().to_dict()
            if var_name == "nov_cat_gmv":
                nov_cat_gmv = cat_data
            elif var_name == "dec_cat_gmv":
                dec_cat_gmv = cat_data
            else:
                feb_cat_gmv = cat_data

        # 일별 GMV 동적 계산 — 2월만 동적 업데이트
        # 11월/12월은 행사 종료된 전체 기간 데이터(하드코딩)를 유지하여
        # 일별 추이 차트·스파크라인에서 전체 기간(15일/10일)을 표시
        global feb_daily_gmv

        # T열 일자별 매출 요약 데이터를 우선 사용 (시트에 정리된 공식 데이터)
        # B열 상품 데이터는 ~29,000행으로 API 응답 크기 제한에 걸릴 수 있으므로
        # T열 요약이 더 안정적
        from googleapiclient.discovery import build as _build
        _creds = get_sheets_credentials()
        daily_summary = None
        if _creds:
            _svc = _build("sheets", "v4", credentials=_creds)
            daily_summary = fetch_daily_gmv_summary(_svc)

        if daily_summary and daily_summary["daily_gmv"]:
            feb_daily_gmv = daily_summary["daily_gmv"]
            feb_total_gmv_val = daily_summary["total"]
            num_days = len(daily_summary["daily_gmv"])
            feb_period = f"2/23 ~ 진행중 ({num_days}일차)"
            print(f"[Sheets] T열 요약 데이터 사용: {num_days}일치, 총 GMV: {feb_total_gmv_val:,}원")
        else:
            # T열 실패 시 B열 상품 데이터에서 집계
            feb_daily = brand_dfs["feb"].groupby("ord_dt")["gmv2"].sum()
            feb_daily_gmv = feb_daily.sort_index().tolist()
            print(f"[Sheets] B열 상품 데이터에서 일별 GMV 집계: {len(feb_daily_gmv)}일치")

        # 브랜드 차트 및 TOP 상품용 df 동적 계산
        global df_nov, df_dec, df_feb
        df_nov = brand_dfs["nov"]
        df_dec = brand_dfs["dec"]
        df_feb = brand_dfs["feb"]

        # 카테고리 목록은 기존 9개 유지 (categories 변수 변경 안 함)

        print(f"[Sheets] 소싱유형·총GMV·카테고리·일별추이 동적 계산 완료 — 2월 총GMV: {feb_total_gmv_val:,}원")
    else:
        # brand_dfs 로드 실패 시에도 T열 요약 데이터는 시도
        try:
            from googleapiclient.discovery import build as _build2
            _creds2 = get_sheets_credentials()
            if _creds2:
                _svc2 = _build2("sheets", "v4", credentials=_creds2)
                daily_summary = fetch_daily_gmv_summary(_svc2)
                if daily_summary and daily_summary["daily_gmv"]:
                    feb_daily_gmv = daily_summary["daily_gmv"]
                    feb_total_gmv_val = daily_summary["total"]
                    num_days = len(daily_summary["daily_gmv"])
                    feb_period = f"2/23 ~ 진행중 ({num_days}일차)"
                    print(f"[Sheets] fallback: T열 요약 데이터 사용: {num_days}일치")
                else:
                    num_days = len(feb_daily_gmv)
            else:
                num_days = len(feb_daily_gmv)
        except Exception as e:
            print(f"[Sheets] fallback T열 로드 실패: {e}")
            num_days = len(feb_daily_gmv)

    # 동기간 라벨 (예: "D+2", "2일차")
    d_label = f"D+{num_days}"
    days_label = f"{num_days}일차"

    # KPI 스파크라인 생성
    nov_sparkline = generate_sparkline_svg(nov_daily_gmv, color=COLORS["11월"])
    dec_sparkline = generate_sparkline_svg(dec_daily_gmv, color=COLORS["12월"])
    feb_sparkline = generate_sparkline_svg(feb_daily_gmv, color=COLORS["2월"])

    # 차트 생성
    daily_chart = create_daily_trend_chart()
    cat_grouped_chart = create_category_grouped()
    cat_stacked_chart = create_category_stacked()
    brand_chart = create_brand_chart()
    sourcing_chart = create_sourcing_charts()

    # 테이블 및 분석 (전체 데이터 사용, 없으면 기존 데이터 fallback)
    common_brands_html = get_common_brands_table(brand_dfs=brand_dfs, d_label=d_label)
    brand_analysis_html = create_brand_analysis(brand_dfs=brand_dfs, d_label=d_label)
    nov_products = create_top_products_table(df_nov, "11월")
    dec_products = create_top_products_table(df_dec, "12월")
    feb_products = create_top_products_table(df_feb, "2월")

    # 동기간 비교 (D+N 동기간 GMV 사용 — 행사 개요 전체 기간과 별도)
    if brand_dfs is not None:
        sp_nov = nov_same_period_gmv
        sp_dec = dec_same_period_gmv
        sp_feb = feb_total_gmv_val
    else:
        sp_nov = nov_daily_gmv[0]
        sp_dec = dec_daily_gmv[0]
        sp_feb = feb_daily_gmv[0]
    sp = {
        "nov_d1": sp_nov,
        "dec_d1": sp_dec,
        "feb_d1": sp_feb,
        "feb_vs_nov": (sp_feb - sp_nov) / sp_nov * 100 if sp_nov else 0,
        "feb_vs_dec": (sp_feb - sp_dec) / sp_dec * 100 if sp_dec else 0,
    }

    # Plotly HTML 변환 (responsive: 컨테이너 크기에 맞춰 자동 조절)
    plotly_config = {"responsive": True, "displayModeBar": False}
    daily_html = daily_chart.to_html(full_html=False, include_plotlyjs=False, config=plotly_config)
    cat_grouped_html = cat_grouped_chart.to_html(full_html=False, include_plotlyjs=False, config=plotly_config)
    cat_stacked_html = cat_stacked_chart.to_html(full_html=False, include_plotlyjs=False, config=plotly_config)
    brand_html = brand_chart.to_html(full_html=False, include_plotlyjs=False, config=plotly_config)
    sourcing_html = sourcing_chart.to_html(full_html=False, include_plotlyjs=False, config=plotly_config)

    # 증분률 표시 헬퍼
    def arrow(pct):
        if pct > 0:
            return f'<span class="up">▲{pct:.1f}%</span>'
        elif pct < 0:
            return f'<span class="down">▼{abs(pct):.1f}%</span>'
        return f'<span>-</span>'

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>리빙 페스타 실적 비교 대시보드</title>
    <link rel="stylesheet" href="https://cdn.jsdelivr.net/gh/orioncactus/pretendard/dist/web/static/pretendard.css">
    <link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Poppins:wght@300;400;600;700;800&display=swap">
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        * {{ margin: 0; padding: 0; box-sizing: border-box; }}
        body {{
            font-family: 'Pretendard', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
            background: #f0f2f5;
            color: #333;
            line-height: 1.6;
        }}
        .container {{ max-width: 1400px; margin: 0 auto; padding: 20px; }}

        /* 애니메이션 정의 */
        @keyframes slideUp {{
            from {{ opacity: 0; transform: translateY(30px); }}
            to {{ opacity: 1; transform: translateY(0); }}
        }}
        @keyframes fadeIn {{
            from {{ opacity: 0; }}
            to {{ opacity: 1; }}
        }}
        @keyframes pulse {{
            0%, 100% {{ opacity: 0.9; }}
            50% {{ opacity: 1; }}
        }}

        /* 헤더 */
        .header {{
            background: linear-gradient(135deg, #5f0080 0%, #9C27B0 100%);
            color: white; padding: 30px 40px; border-radius: 16px;
            margin-bottom: 24px; box-shadow: 0 4px 20px rgba(95,0,128,0.3);
        }}
        .header h1 {{ font-size: 28px; font-weight: 700; margin-bottom: 8px; }}
        .header p {{ font-size: 14px; opacity: 0.9; }}
        .header .badge {{
            display: inline-block; background: rgba(255,255,255,0.15);
            padding: 4px 12px; border-radius: 20px; font-size: 11px; margin-top: 8px;
            opacity: 0.85;
        }}
        .header .update-badge {{
            display: inline-block; background: rgba(255,255,255,0.95);
            color: #5f0080; padding: 8px 20px; border-radius: 24px;
            font-size: 14px; font-weight: 700; margin-top: 12px;
            box-shadow: 0 2px 12px rgba(0,0,0,0.15);
            animation: pulse 3s ease-in-out infinite;
            letter-spacing: -0.3px;
        }}
        .header .update-badge .update-icon {{
            margin-right: 6px;
        }}

        /* 섹션 */
        .section {{
            background: white; border-radius: 16px; padding: 28px;
            margin-bottom: 24px; box-shadow: 0 2px 12px rgba(0,0,0,0.06);
            animation: fadeIn 0.6s ease-out both;
        }}
        .section-title {{
            font-size: 20px; font-weight: 700; color: #1a1a2e;
            margin-bottom: 20px; padding-bottom: 12px;
            border-bottom: 2px solid #eee;
            display: flex; align-items: center; gap: 8px;
        }}
        .section-title .icon {{ font-size: 24px; }}

        /* 차트 컨테이너 */
        .chart-wrap {{
            width: 100%;
        }}
        .section .js-plotly-plot,
        .section .plotly-graph-div {{
            width: 100% !important;
        }}
        .section .svg-container {{
            width: 100% !important;
        }}

        /* KPI 카드 */
        .kpi-grid {{
            display: grid; grid-template-columns: repeat(3, 1fr);
            gap: 20px; margin-bottom: 16px;
        }}
        .kpi-card {{
            border-radius: 14px; padding: 24px;
            position: relative; overflow: hidden;
            animation: slideUp 0.5s ease-out both;
        }}
        .kpi-card:nth-child(1) {{ animation-delay: 0s; }}
        .kpi-card:nth-child(2) {{ animation-delay: 0.12s; }}
        .kpi-card:nth-child(3) {{ animation-delay: 0.24s; }}
        .kpi-card.nov {{ background: linear-gradient(135deg, #5f008015, #5f008008); border: 1px solid #5f008030; }}
        .kpi-card.dec {{ background: linear-gradient(135deg, #F57C0015, #F57C0008); border: 1px solid #F57C0030; }}
        .kpi-card.feb {{ background: linear-gradient(135deg, #00897B15, #00897B08); border: 1px solid #00897B30; }}
        .kpi-card h3 {{ font-size: 16px; color: #666; margin-bottom: 14px; }}
        .kpi-card .kpi-value {{ font-size: 28px; font-weight: 800; color: #1a1a2e; font-family: 'Poppins', sans-serif; }}
        .kpi-card .kpi-sub {{ font-size: 13px; color: #888; margin-top: 8px; }}
        .kpi-card .kpi-detail {{
            margin-top: 12px; font-size: 13px; color: #555;
            display: flex; flex-direction: column; gap: 4px;
        }}
        .kpi-card .kpi-detail span {{ display: flex; justify-content: space-between; }}
        .kpi-card .sparkline {{ margin-top: 12px; opacity: 0.85; }}

        /* 동기간 비교 */
        .comparison-box {{
            background: #F3E5F5; border: 1px solid #CE93D8; border-radius: 12px;
            padding: 20px; margin-bottom: 20px;
        }}
        .comparison-box h3 {{ font-size: 16px; color: #5f0080; margin-bottom: 12px; }}
        .comparison-grid {{
            display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px;
        }}
        .comparison-item {{ text-align: center; }}
        .comparison-item .label {{ font-size: 12px; color: #888; }}
        .comparison-item .value {{ font-size: 22px; font-weight: 700; color: #333; font-family: 'Poppins', sans-serif; }}
        .comparison-item .change {{ font-size: 13px; margin-top: 4px; }}

        .up {{ color: #E53935; font-weight: 600; }}
        .down {{ color: #1E88E5; font-weight: 600; }}

        /* 테이블 */
        .data-table {{
            width: 100%; border-collapse: collapse; font-size: 13px;
        }}
        .data-table th {{
            background: #f8f9fa; padding: 10px 12px; text-align: left;
            font-weight: 600; color: #555; border-bottom: 2px solid #dee2e6;
            position: sticky; top: 0;
        }}
        .data-table td {{
            padding: 9px 12px; border-bottom: 1px solid #eee;
        }}
        .data-table tr:hover {{ background: #F3E5F520; }}
        .data-table .num {{ text-align: right; font-variant-numeric: tabular-nums; font-family: 'Poppins', sans-serif; }}
        .data-table .rank {{ text-align: center; font-weight: 700; color: #5f0080; }}
        .data-table .product-name {{
            max-width: 280px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
        }}
        .data-table .brand-name {{ font-weight: 600; }}
        .sourcing-1p {{ color: #5f0080; font-weight: 600; }}
        .sourcing-3p {{ color: #F57C00; font-weight: 600; }}
        .sourcing-3pl {{ color: #00897B; font-weight: 600; }}

        /* 프로그레스 바 (TOP 상품 GMV 셀) */
        .gmv-bar {{
            position: relative; text-align: right; font-variant-numeric: tabular-nums;
            font-family: 'Poppins', sans-serif; padding: 9px 12px;
        }}
        .gmv-bar .bar-bg {{
            position: absolute; left: 0; top: 0; bottom: 0;
            background: linear-gradient(90deg, rgba(95,0,128,0.10), rgba(95,0,128,0.04));
            border-radius: 0 4px 4px 0; z-index: 0;
            transition: width 0.6s ease-out;
        }}
        .gmv-bar .bar-text {{ position: relative; z-index: 1; }}

        /* 탭 */
        .tab-buttons {{
            display: flex; gap: 8px; margin-bottom: 16px;
        }}
        .tab-btn {{
            padding: 8px 20px; border: 2px solid #ddd; border-radius: 8px;
            background: white; cursor: pointer; font-size: 14px; font-weight: 600;
            transition: all 0.2s;
        }}
        .tab-btn:hover {{ border-color: #5f0080; color: #5f0080; transform: translateY(-2px); box-shadow: 0 4px 12px rgba(95,0,128,0.15); }}
        .tab-btn.active {{ background: #5f0080; color: white; border-color: #5f0080; }}
        .tab-content {{ display: none; }}
        .tab-content.active {{ display: block; }}

        /* 소싱유형 증분 테이블 */
        .sourcing-summary {{
            display: grid; grid-template-columns: repeat(3, 1fr); gap: 16px;
            margin-top: 40px;
        }}
        .sourcing-item {{
            background: #f3edf7; border-radius: 10px; padding: 18px;
            text-align: center; border: 1px solid #e0d4e8;
        }}
        .sourcing-item .label {{ font-size: 15px; font-weight: 700; color: #5f0080; margin-bottom: 8px; }}
        .sourcing-item .amounts {{ font-size: 14px; color: #333; font-weight: 500; line-height: 1.8; }}

        /* 기간 배지 */
        .period-badge {{
            display: inline-block; padding: 2px 10px; border-radius: 12px;
            font-size: 12px; font-weight: 600; letter-spacing: 0.02em;
        }}
        .period-badge.ongoing {{
            background: #00BFA520; color: #00897B; border: 1px solid #00BFA550;
            animation: pulse 2s ease-in-out infinite;
        }}
        .period-badge.completed {{
            background: #f0f0f0; color: #888; border: 1px solid #ddd;
        }}

        /* 브랜드 검색/정렬 컨트롤 */
        .brand-controls {{
            display: flex; align-items: center; gap: 12px;
            margin-bottom: 14px; flex-wrap: wrap;
        }}
        .brand-search {{
            flex: 1; min-width: 200px; padding: 9px 14px;
            border: 2px solid #ddd; border-radius: 10px;
            font-size: 14px; outline: none; transition: border 0.2s;
            font-family: inherit;
        }}
        .brand-search:focus {{ border-color: #5f0080; }}
        .brand-search::placeholder {{ color: #bbb; }}
        .brand-sort-buttons {{ display: flex; gap: 6px; }}
        .sort-btn {{
            padding: 7px 16px; border: 2px solid #ddd; border-radius: 8px;
            background: white; cursor: pointer; font-size: 13px;
            font-weight: 600; color: #666; transition: all 0.2s;
            font-family: inherit;
        }}
        .sort-btn:hover {{ border-color: #5f0080; color: #5f0080; }}
        .sort-btn.active {{ background: #5f0080; color: white; border-color: #5f0080; }}

        /* 페이지네이션 */
        .brand-pagination {{
            display: flex; justify-content: center; align-items: center;
            gap: 4px; margin-top: 16px; flex-wrap: wrap;
        }}
        .page-btn {{
            padding: 6px 12px; border: 1px solid #ddd; border-radius: 6px;
            background: white; cursor: pointer; font-size: 13px;
            font-weight: 500; color: #555; transition: all 0.2s;
            font-family: 'Poppins', sans-serif;
        }}
        .page-btn:hover:not(.disabled):not(.active) {{ border-color: #5f0080; color: #5f0080; background: #F3E5F510; }}
        .page-btn.active {{ background: #5f0080; color: white; border-color: #5f0080; }}
        .page-btn.disabled {{ color: #ccc; cursor: default; }}

        /* 결과 카운트 */
        .brand-result-count {{
            text-align: center; font-size: 12px; color: #999;
            margin-top: 8px;
        }}

        /* 신규 브랜드 태그 & 토글 */
        .new-brand-tag {{
            background: #00BFA510; border: 1px solid #00BFA540;
            border-radius: 8px; padding: 6px 14px; font-size: 13px;
        }}
        .toggle-new-brands-btn {{
            display: inline-block; margin-top: 12px; padding: 7px 20px;
            border: 2px solid #00BFA540; border-radius: 8px;
            background: white; cursor: pointer; font-size: 13px;
            font-weight: 600; color: #00897B; transition: all 0.2s;
            font-family: inherit;
        }}
        .toggle-new-brands-btn:hover {{ background: #00BFA510; border-color: #00897B; }}

        /* 반응형: 태블릿 (768px 이하) */
        @media (max-width: 768px) {{
            .container {{ padding: 12px; }}
            .header {{ padding: 20px 20px; border-radius: 12px; }}
            .header h1 {{ font-size: 22px; }}
            .section {{ padding: 16px; border-radius: 12px; }}
            .section-title {{ font-size: 17px; }}
            .kpi-grid {{ grid-template-columns: 1fr; gap: 12px; }}
            .kpi-card {{ padding: 18px; }}
            .kpi-card .kpi-value {{ font-size: 24px; }}
            .comparison-grid {{ grid-template-columns: 1fr; }}
            .comparison-item .value {{ font-size: 20px; }}
            .sourcing-summary {{ grid-template-columns: 1fr; }}
            .tab-buttons {{ flex-wrap: wrap; }}
            .tab-btn {{ padding: 8px 14px; font-size: 13px; flex: 1; min-width: 0; text-align: center; }}
            .brand-controls {{ flex-direction: column; }}
            .brand-search {{ min-width: 100%; }}
            .brand-sort-buttons {{ width: 100%; }}
            .sort-btn {{ flex: 1; text-align: center; }}
            .data-table {{ font-size: 12px; }}
            .data-table th, .data-table td {{ padding: 7px 6px; }}
            .data-table .product-name {{ max-width: 160px; }}
        }}

        /* 반응형: 모바일 (480px 이하) */
        @media (max-width: 480px) {{
            .container {{ padding: 6px; }}
            .header {{ padding: 16px; border-radius: 10px; }}
            .header h1 {{ font-size: 18px; }}
            .header p {{ font-size: 12px; }}
            .header .badge {{ font-size: 10px; padding: 3px 8px; }}
            .section {{ padding: 10px; margin-bottom: 12px; }}
            .section-title {{ font-size: 15px; margin-bottom: 10px; }}
            .section-title .icon {{ font-size: 18px; }}
            .kpi-card {{ padding: 14px; }}
            .kpi-card h3 {{ font-size: 14px; }}
            .kpi-card .kpi-value {{ font-size: 22px; }}
            .kpi-card .kpi-detail {{ font-size: 12px; }}
            .comparison-box {{ padding: 12px; }}
            .comparison-box h3 {{ font-size: 14px; }}
            .comparison-item .value {{ font-size: 18px; }}
            .tab-btn {{ padding: 7px 10px; font-size: 12px; }}
            .data-table {{ font-size: 11px; }}
            .data-table th, .data-table td {{ padding: 6px 4px; }}
            .data-table .product-name {{ max-width: 120px; }}
            .data-table .brand-name {{ max-width: 80px; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }}
            .page-btn {{ padding: 5px 8px; font-size: 12px; }}
            .sort-btn {{ padding: 6px 10px; font-size: 12px; }}
            .new-brand-tag {{ padding: 4px 10px; font-size: 12px; }}
            .sourcing-item {{ padding: 10px; }}
        }}

        /* 테이블 가로 스크롤 컨테이너 */
        .table-scroll {{ overflow-x: auto; -webkit-overflow-scrolling: touch; }}
    </style>
</head>
<body>
<!-- 비밀번호 잠금 화면 -->
<div id="lock-screen" style="
    position:fixed; top:0; left:0; width:100%; height:100%; z-index:9999;
    background:linear-gradient(135deg, #5f0080 0%, #9C27B0 100%);
    display:flex; align-items:center; justify-content:center;
">
    <div style="background:white; border-radius:20px; padding:30px 24px; text-align:center; box-shadow:0 8px 40px rgba(0,0,0,0.3); max-width:380px; width:90%;">
        <div style="font-size:40px; margin-bottom:12px;">🔒</div>
        <h2 style="font-size:20px; font-weight:700; color:#1a1a2e; margin-bottom:8px;">리빙 페스타 대시보드</h2>
        <p style="font-size:13px; color:#888; margin-bottom:24px;">내부 분석용 — 비밀번호를 입력하세요</p>
        <input id="pw-input" type="password" placeholder="비밀번호 입력"
            style="width:100%; padding:12px 16px; border:2px solid #ddd; border-radius:10px;
            font-size:16px; text-align:center; outline:none; transition:border 0.2s;"
            onfocus="this.style.borderColor='#5f0080'" onblur="this.style.borderColor='#ddd'"
        >
        <div id="pw-error" style="color:#E53935; font-size:13px; margin-top:8px; display:none;">비밀번호가 틀렸습니다</div>
        <button onclick="checkPassword()" style="
            width:100%; margin-top:16px; padding:12px; background:#5f0080; color:white;
            border:none; border-radius:10px; font-size:15px; font-weight:600; cursor:pointer;
            transition:background 0.2s;
        " onmouseover="this.style.background='#7B1FA2'" onmouseout="this.style.background='#5f0080'">확인</button>
    </div>
</div>

<!-- 대시보드 본문 (잠금 해제 후 표시) -->
<div class="container" id="dashboard" style="display:none;">
    <!-- 헤더 -->
    <div class="header">
        <h1>리빙 페스타 실적 비교 대시보드</h1>
        <p>11월 블랙위크 vs 12월 리빙페스타 vs 2월 리빙페스타 행사 실적 비교 분석</p>
        <span class="badge">생활그룹 내부 분석용</span>
        <br>
        <span class="update-badge"><span class="update-icon">🔄</span> 데이터 업데이트: {pd.Timestamp.now(tz='Asia/Seoul').strftime('%Y-%m-%d %H:%M')} KST</span>
    </div>

    <!-- 섹션 1: 행사 개요 KPI 카드 -->
    <div class="section">
        <div class="section-title"><span class="icon">📊</span> 행사 개요</div>

        <!-- 월별 실적 KPI 카드 (2월 → 12월 → 11월 순서) -->
        <div class="kpi-grid">
            <div class="kpi-card feb">
                <h3>2월 리빙페스타</h3>
                <div class="kpi-value">{format_krw_full(feb_total_gmv_val)}</div>
                <div class="kpi-sub"><span class="period-badge ongoing">{feb_period.split('(')[-1].replace(')', '') if '(' in feb_period else '진행중'}</span></div>
                <div class="kpi-detail">
                    <span><b>기간:</b> <em>{feb_period}</em></span>
                    <span><b>vs 12월 동기간:</b> <em>{arrow(sp['feb_vs_dec'])}</em></span>
                </div>
                <div class="sparkline">{feb_sparkline}</div>
            </div>
            <div class="kpi-card dec">
                <h3>12월 리빙페스타</h3>
                <div class="kpi-value">{format_krw_full(dec_total_gmv_val)}</div>
                <div class="kpi-sub"><span class="period-badge completed">전체 기간</span></div>
                <div class="kpi-detail">
                    <span><b>기간:</b> <em>{dec_period}</em></span>
                </div>
                <div class="sparkline">{dec_sparkline}</div>
            </div>
            <div class="kpi-card nov">
                <h3>11월 블랙위크</h3>
                <div class="kpi-value">{format_krw_full(nov_total_gmv)}</div>
                <div class="kpi-sub"><span class="period-badge completed">전체 기간</span></div>
                <div class="kpi-detail">
                    <span><b>기간:</b> <em>{nov_period}</em></span>
                </div>
                <div class="sparkline">{nov_sparkline}</div>
            </div>
        </div>

        <!-- 동기간 비교 (2월 → 12월 → 11월 순서) -->
        <div class="comparison-box">
            <h3>⭐ {d_label} 동기간 비교 ({days_label} 누적 기준)</h3>
            <div class="comparison-grid">
                <div class="comparison-item">
                    <div class="label">2월 리빙페스타 {d_label}</div>
                    <div class="value">{format_krw_full(sp['feb_d1'])}</div>
                    <div class="change">vs 12월 {arrow(sp['feb_vs_dec'])}</div>
                </div>
                <div class="comparison-item">
                    <div class="label">12월 리빙페스타 {d_label}</div>
                    <div class="value">{format_krw_full(sp['dec_d1'])}</div>
                    <div class="change">vs 11월 {arrow((sp['dec_d1']-sp['nov_d1'])/sp['nov_d1']*100)}</div>
                </div>
                <div class="comparison-item">
                    <div class="label">11월 블랙위크 {d_label}</div>
                    <div class="value">{format_krw_full(sp['nov_d1'])}</div>
                </div>
            </div>
        </div>
    </div>

    <!-- 섹션 2: 동일자(D-day) 매출 추이 비교 -->
    <div class="section">
        <div class="section-title"><span class="icon">📈</span> 동일자(D-day) 매출 추이 비교</div>
        <p style="font-size:13px; color:#888; margin-bottom:12px;">
            각 행사 시작일 기준 동기간({days_label}) 매출 추이를 비교합니다. (11월: 11/17~, 12월: 12/22~, 2월: 2/23~)
        </p>
        <div class="chart-wrap">{daily_html}</div>
    </div>

    <!-- 섹션 3: 카테고리별 매출 비교 -->
    <div class="section">
        <div class="section-title"><span class="icon">📦</span> 카테고리별 매출 비교</div>
        <p style="font-size:13px; color:#888; margin-bottom:12px;">
            동기간 {days_label} 누적 기준 카테고리별 매출을 비교합니다.
        </p>
        <div class="chart-wrap">{cat_grouped_html}</div>
    </div>

    <!-- 섹션 4: 브랜드별 매출 비교 -->
    <div class="section">
        <div class="section-title"><span class="icon">🏷️</span> 브랜드별 매출 비교</div>
        <p style="font-size:13px; color:#888; margin-bottom:12px; line-height:1.8;">
            버튼을 클릭하여 월별 TOP 10 브랜드를 확인할 수 있습니다.<br>
            동기간 {days_label} 누적 기준 브랜드별 매출입니다.
        </p>
        <div class="chart-wrap">{brand_html}</div>

        <div style="margin-top:28px; padding-top:20px; border-top:1px solid #eee;">
            <h4 style="font-size:16px; font-weight:700; color:#1a1a2e; margin-bottom:16px;">브랜드 실적 비교 분석</h4>
            <p style="font-size:13px; color:#888; margin-bottom:16px;">
                각 행사 동기간({days_label} 누적) 기준 브랜드별 실적을 비교합니다.
            </p>
            {brand_analysis_html}
        </div>
    </div>

    <!-- 섹션 5: 소싱유형별 비교 -->
    <div class="section">
        <div class="section-title"><span class="icon">🔄</span> 소싱유형별 비교</div>
        <div class="chart-wrap">{sourcing_html}</div>

        <div class="sourcing-summary">
            <div class="sourcing-item">
                <div class="label">1p</div>
                <div class="amounts">
                    2월: {format_krw_full(feb_sourcing['1p'])}<br>
                    12월: {format_krw_full(dec_sourcing['1p'])}<br>
                    11월: {format_krw_full(nov_sourcing['1p'])}
                </div>
            </div>
            <div class="sourcing-item">
                <div class="label">3p</div>
                <div class="amounts">
                    2월: {format_krw_full(feb_sourcing['3p'])}<br>
                    12월: {format_krw_full(dec_sourcing['3p'])}<br>
                    11월: {format_krw_full(nov_sourcing['3p'])}
                </div>
            </div>
            <div class="sourcing-item">
                <div class="label">3pl</div>
                <div class="amounts">
                    2월: {format_krw_full(feb_sourcing['3pl'])}<br>
                    12월: {format_krw_full(dec_sourcing['3pl'])}<br>
                    11월: {format_krw_full(nov_sourcing['3pl'])}
                </div>
            </div>
        </div>
    </div>

    <!-- 섹션 6: TOP 상품 비교 테이블 -->
    <div class="section">
        <div class="section-title"><span class="icon">🏆</span> TOP 상품 비교</div>
        <p style="font-size:13px; color:#888; margin-bottom:12px; line-height:1.8;">
            동기간 {days_label} 누적 기준 TOP 상품입니다.
            &nbsp;&nbsp;<span style="color:#bbb;">※ 행사 시작일부터 {days_label} 누적</span>
        </p>
        <div class="tab-buttons">
            <button class="tab-btn active" onclick="switchTab('2월')">2월 리빙페스타</button>
            <button class="tab-btn" onclick="switchTab('12월')">12월 리빙페스타</button>
            <button class="tab-btn" onclick="switchTab('11월')">11월 블랙위크</button>
        </div>
        <div style="overflow-x:auto;">
            {feb_products.replace('class="tab-content"', 'class="tab-content active"')}
            {dec_products}
            {nov_products}
        </div>
    </div>

    <!-- 푸터 -->
    <div style="text-align:center; padding:20px; color:#999; font-size:12px;">
        생활그룹 리빙 페스타 실적 대시보드 | 데이터 소스: Google Sheets | 생성일: 2026-02-24
    </div>
</div>

<script>
// 비밀번호 확인
function checkPassword() {{
    var pw = document.getElementById('pw-input').value;
    if (pw === '1234') {{
        document.getElementById('lock-screen').style.display = 'none';
        document.getElementById('dashboard').style.display = 'block';
        // display:none 상태에서 렌더링된 차트를 올바른 크기로 재조정
        setTimeout(function() {{
            document.querySelectorAll('.plotly-graph-div').forEach(function(gd) {{
                Plotly.Plots.resize(gd);
            }});
            // 리사이즈 완료 후 모바일 최적화 실행
            setTimeout(optimizeChartsForMobile, 100);
        }}, 50);
    }} else {{
        document.getElementById('pw-error').style.display = 'block';
        document.getElementById('pw-input').value = '';
        document.getElementById('pw-input').style.borderColor = '#E53935';
        setTimeout(function() {{ document.getElementById('pw-input').style.borderColor = '#ddd'; }}, 1500);
    }}
}}
// Enter 키로도 비밀번호 제출
document.getElementById('pw-input').addEventListener('keypress', function(e) {{
    if (e.key === 'Enter') checkPassword();
}});

function switchTab(month) {{
    // 탭 버튼 활성화 (정확한 매칭: "2월"이 "12월"에 포함되는 문제 방지)
    document.querySelectorAll('.tab-btn').forEach(btn => {{
        btn.classList.remove('active');
        if (btn.textContent.startsWith(month)) btn.classList.add('active');
    }});
    // 탭 컨텐츠 표시
    document.querySelectorAll('.tab-content').forEach(content => {{
        content.classList.remove('active');
        if (content.id === 'tab-' + month) content.classList.add('active');
    }});
}}

// 모바일 차트 최적화: 차트별 마진/폰트 세밀 조정
function optimizeChartsForMobile() {{
    var w = window.innerWidth;
    var isMobile = w <= 768;
    var isSmall = w <= 480;

    document.querySelectorAll('.plotly-graph-div').forEach(function(gd, idx) {{
        if (!gd.layout) {{ Plotly.Plots.resize(gd); return; }}

        if (isMobile) {{
            var update = {{
                'font.size': isSmall ? 9 : 10,
                'legend.font.size': isSmall ? 8 : 9
            }};

            // 파이/도넛 차트 (소싱유형) — 마진 최소화
            if (gd.data && gd.data[0] && gd.data[0].type === 'pie') {{
                update['margin.l'] = 5;
                update['margin.r'] = 5;
                update['margin.t'] = 40;
                update['margin.b'] = 30;
            }}
            // 수평 바 차트 (브랜드) — 왼쪽 마진 유지, 오른쪽 확보
            else if (gd.data && gd.data[0] && gd.data[0].orientation === 'h') {{
                update['margin.l'] = isSmall ? 60 : 70;
                update['margin.r'] = isSmall ? 30 : 40;
                update['margin.t'] = 70;
                update['margin.b'] = 20;
            }}
            // 일반 차트 (일별 추이, 카테고리)
            else {{
                update['margin.l'] = isSmall ? 25 : 35;
                update['margin.r'] = 10;
                update['margin.t'] = 45;
                update['margin.b'] = 30;
            }}

            Plotly.relayout(gd, update);
        }}
        Plotly.Plots.resize(gd);
    }});
}}

// 차트 최적화는 비밀번호 해제 후 실행 (checkPassword 내에서 호출)

// 화면 크기 변경 시 재최적화 (디바운스 적용)
var resizeTimer;
window.addEventListener('resize', function() {{
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(optimizeChartsForMobile, 200);
}});
</script>
</body>
</html>"""

    return html


# =============================================================================
# 실행
# =============================================================================
if __name__ == "__main__":
    print("대시보드 생성 중...")
    html_content = build_html()

    # 출력 경로: 환경변수 또는 스크립트 기준 상대 경로
    output_path = os.environ.get(
        "DASHBOARD_OUTPUT_PATH",
        str(_SCRIPT_DIR / "festa_dashboard.html")
    )
    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html_content)

    # docs/index.html 자동 복사 (docs 폴더가 있으면)
    docs_path = _SCRIPT_DIR / "docs" / "index.html"
    if docs_path.parent.exists():
        with open(docs_path, "w", encoding="utf-8") as f:
            f.write(html_content)
        print(f"GitHub Pages 업데이트: {docs_path}")

    print(f"대시보드가 생성되었습니다: {output_path}")
