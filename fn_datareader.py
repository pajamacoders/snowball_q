# 1996-12-11~
import time
import FinanceDataReader as fdr # 현재 시장 상장사 및 상장사 주식 가격 정보
import dart_fss as dfss
import OpenDartReader # 재무 데이터 
import requests
from bs4 import BeautifulSoup
# from cus_stock import CStock
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from financial_statement import Company, FStatements, Base
from extract_financial_factor import *
import pickle
from keys import *
from extract_fs_info_from_dart import decompose_report_to_fs_is_cf
import json
import glob
import os
dfss.set_api_key(api_key=api_key)
dart = OpenDartReader(api_key) 

# SQLite 데이터베이스에 연결하는 엔진 생성
engine = create_engine(SQLALCHEMY_DATABASE_URL, echo=True)

# 데이터베이스와 테이블 생성
# Base.metadata.create_all(engine)

# 세션 생성
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# session = SessionLocal()

def check_suffix(value, suffix):
    return value.endswith(suffix)

def get_corp_list():
    stock_list = fdr.StockListing("KRX")
    # remove konex
    stock_list = stock_list.loc[stock_list["Market"] != "KONEX"]
    # remove stock without region because this kind of stock is not a normal company stock
    # stock_list =  stock_list.loc[stock_list["Region"].notnull()]
    stock_list = filter_non_corporation(stock_list)
    stock_list = stock_list.sort_values(by='Marcap', ascending=False)
    return stock_list[-20:]

def filter_non_corporation(stock_list):
    """dataframe contains stocklist
    """
    target_suffix = '0'
    prior_share_mask = stock_list.Code.apply(lambda x: check_suffix(x, target_suffix))
    stock_list = stock_list[prior_share_mask]
    #리츠주 제거, 리츠 주는 이름에 대부분 "n호" 라는 패턴이 들어가므로 이를 이요해 제거
    reit_pattern = r'\d+호' 
    stock_list = stock_list[~stock_list["Name"].str.contains(reit_pattern)]
    # 스팩 주 제거
    spac_pattern = r'스팩' 
    stock_list = stock_list[~stock_list["Name"].str.contains(spac_pattern)]
    return stock_list

def get_copr_list_of_bottm_20percent_marcap(corp_list, btm_ratio:float=0.2):
    """
    상장 기업 중 시가총액 기준 하위 btm_ratio 에 해당하는 기업 리스트 리턴
    """
    btm_25per_idx = int(len(corp_list)*btm_ratio)
    small_cap = corp_list[-btm_25per_idx:]
    small_corp_info = []
    for symbol, name, marcap, n_shares, market in small_cap:
        pass
    #    small_corp_info.append(CStock(symbol, name, marcap, n_shares, market, dart))
    small_corp_info = [corp for corp in small_corp_info if corp.is_report_availiable()]
    return small_corp_info

def fetch_listing_date(company_code):
    api_url = 'https://api.openapipartners.com/public_info/v1/company_listing_date/{}'.format(company_code)
    headers = {
        'Authorization': 'Bearer {}'.format(api_key),
    }

    response = requests.get(api_url, headers=headers)

    if response.status_code == 200:
        response_data = response.json()
        listing_date = response_data['listing_date']  # listing_date에 실제 필드명을 사용
        return listing_date
    else:
        return None
# reprt_code  보고서 코드  STRING(5) Y 1분기보고서 : 11013
# 반기보고서 : 11012
# 3분기보고서 : 11014
# 사업보고서 : 11011

def add_total_stock_quantity_status(info:dict, corp_code, year, reprt_code):
    stock_status=dfss.api.info.stock_totqy_sttus(corp_code, str(year), reprt_code=reprt_code)
    if stock_status.get('status')=='000':
        for status in stock_status['list']:
            if status['se']=='보통주':
                info['issued_shares'] = int(status['istc_totqy'].replace(',','')) # 발행주식의 총수
                try:
                    info['treasury_shares'] = int(status['tesstk_co'].replace(',','')) #자기주식수
                except ValueError as e:
                    info['treasury_shares']=0
                info['outstanding_shares'] = int(status['distb_stock_co'].replace(',','')) #유통주식수
    else:
        info['issued_shares']=-1
        info['treasury_shares']=-1
        info['outstanding_shares']=-1
    return info

def write_to_file(corp_status, file_name):
    with open(file_name, 'w') as f:
        json.dump(corp_status, f, ensure_ascii=False)
    
        
def read_from_file(file_name):
    with open(file_name, 'r') as f:
        contents = json.load(f)
    return contents

def handled_corp_list(path):
    corp_list = glob.glob(os.path.join(path,'*.json'))
    corp_list=[os.path.basename(filename).replace('.json','') for filename in corp_list]
    return corp_list

if __name__=="__main__":
    #기업정보
    st_list = get_corp_list()
    crp_list=dfss.get_corp_list()
    handled_corp_code_list = handled_corp_list('data_files')
    for idx, row in st_list.iterrows():
        corp = crp_list.find_by_stock_code(row.Code)
        if corp.corp_code in handled_corp_code_list:
            continue
        # listing_date = fetch_listing_date(corp.corp_code)
        #dart.company('005930')
        corp_info=dict(meta={'corp_code':corp.corp_code,
        'stock_code':row.Code,
         'corp_name':row.Name,
         'market': row.Market,
         'sector': corp.sector})
        reports = corp.search_filings(bgn_de='20100101', pblntf_detail_ty=['a003'], \
            last_reprt_at='Y', page_no=1, page_count=200)
        begin_year = int(reports[-1].rcept_dt[:4])
        fs_quarter=None
        # for year in range(begin_year, 2023):
        #     try:
        #         fs_quarter = corp.extract_fs(bgn_de=year, report_tp='quarter')
        #         break
        #     except Exception as e:
        #         print(e)
        #         print(f'-------------------ERROR:{year}-----------------')
                
        if fs_quarter is not None:
            fs_list = get_financial_statements_factor_vb2015(row.Code, fs_quarter)
            dbs = fs_quarter['bs']
            # dbs_columns = dbs.columns.to_flat_index()[7:]
            #bs_labels = fs_quarter.labels['bs']
            # dis=fs_quarter['is']
            # is_labels = dis.columns.to_flat_index()
            is_list = get_income_statements_factor(row.Code, fs_quarter)
            dcf=fs_quarter['bs']
            cf_labels = dcf.columns.to_flat_index()
            print(corp)
        else:
            total_res=[]
            for year in range(begin_year, 2024):
                status_results=[]
                for i, code in enumerate(['11013','11012', '11014', '11011']):
                    q_report_all=dart.finstate_all(corp.corp_code, year, reprt_code=code)
                    if q_report_all.shape[0]==0:
                        q_report_all=dart.finstate_all(corp.corp_code, year, reprt_code=code, fs_div='OFS')
                    # q_report=dart.finstate(corp.corp_code, year, reprt_code=code)
                    if q_report_all.shape[0]!=0:
                        info = decompose_report_to_fs_is_cf(corp.corp_code, year, i+1, q_report_all)
                        info = add_total_stock_quantity_status(info, corp.corp_code, \
                            year, reprt_code=code)
                        status_results.append(info)

                # 보고서의 현금흐름표는 각 분기의 누적치(반기보고서는 1~6월의 현금흐름, 3분기 보고서는 1~9월의 현금흐름)
                # 이므로 전분기값을 빼서 각 분기의 값을 구함
                for i in range(len(status_results)-1, 0,-1):
                    status_results[i]['cfo']-=status_results[i-1]['cfo']
                    status_results[i]['cfi']-=status_results[i-1]['cfi']
                    status_results[i]['cff']-=status_results[i-1]['cff']
                # 4분기 보고서가 4개일 경우 4분기 보고서는 사업보고서로
                # 손익계산서에 1년 누적치를 가지고 있으므로 1~3분기의 각 계정 값을 빼서 4분기 값을 구함
                # 손익계정 list
                is_keys=['revenue','gross_progit','operating_income','finance_cost',
                         'income_befor_tax', 'net_profit','diluted_eps']
                if len(status_results)==4 and status_results[-1]['quarter']==4:
                    for i in range(2,-1,-1):
                        for k in is_keys:
                            status_results[-1][k] -= status_results[i][k]
                total_res+=status_results
                
            corp_info['fn_state']=total_res
            write_to_file(corp_info, f"data_files/{corp.corp_code}.json")
            handled_corp_code_list.append(corp.corp_code)
            time.sleep(20)

    print(st_list)
