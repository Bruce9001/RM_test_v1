import datetime
from dateutil.relativedelta import relativedelta

#날짜 입력하여 월 시작일, 종료일 확인
def get_start_end_dates(year, month):
    # 월의 시작 날짜: 항상 1일
    start_date = datetime.date(year, month, 1)
    # 다음 달의 첫 날짜 계산
    if month == 12:
        # 12월인 경우, 다음 해의 1월 1일을 계산
        next_month = datetime.date(year + 1, 1, 1)
    else:
        # 그 외의 경우, 다음 달의 1일을 계산
        next_month = datetime.date(year, month + 1, 1)
    # 월의 마지막 날짜: 다음 달의 첫 날에서 하루를 빼면 됨
    end_date = next_month + relativedelta(days=-1)
    return start_date, end_date
