import numpy as np
fs_fac_idx_b2015={"유동자산":0,
 "비유동자산":10,
 "자산총계":19,
 "유동부채":20,
 "비유동부채":31,
 "부채총계":40,
 "자본금":42,
 "이익잉여금":46,
 "자본총계":49}

fs_fac_idx_a2015={"유동자산":0,
 "비유동자산":11,
 "자산총계":21,
 "유동부채":22,
 "비유동부채":34,
 "부채총계":42,
 "자본금":44,
 "이익잉여금":48,
 "자본총계":52}

is_fac_idx_b2015={"매출":0, # revenue
                  "매출총이익":2, #gross_profit
                  "영업이익":4, #operating_income
                  "금융비용":9, #finance_cost
                  "법인세비용차감전순이익":10, #income_befor_tax
                  "당기순이익":13, #net_profit
                  "희석주당이익":17 #diluted_eps
                  }

month2quater={1:1, 2:1, 3:1,
              4:2, 5:2, 6:2,
              7:3, 8:3, 9:3,
              10:4, 11:4, 12:4}

def get_financial_statements_factor_vb2015(corp_code, fs_list):
    """extract financial statement factor from report issued before 2015
    """
    dbs = fs_list['bs']
    #bs_labels = fs_quarter.labels['bs'] # 라벨 정보
    dbs_columns = dbs.columns.to_flat_index()[7:]
    num_columns = len(dbs_columns)
    res_fs_list = []
    for i in range(7, 7+num_columns,1):
        fs_factors = dbs.iloc[:,i]
        res_fs_list.append(
        {"stock_code":corp_code,
        "year":int(dbs_columns[i-7][0][:4]),
        "quater":month2quater[int(dbs_columns[i-7][0][4:6])],
        "current_assets":fs_factors[fs_fac_idx_b2015["유동부채"]],
        "noncurrent_asset":fs_factors[fs_fac_idx_b2015["비유동부채"]],
        "full_assets":fs_factors[fs_fac_idx_b2015["자산총계"]],
        "current_liabilities":fs_factors[fs_fac_idx_b2015["유동부채"]],
        "noncurrent_liabilities":fs_factors[fs_fac_idx_b2015["비유동부채"]],
        "full_liabilities":fs_factors[fs_fac_idx_b2015["부채총계"]],
        "issued_capital":fs_factors[fs_fac_idx_b2015["자본금"]],
        "retain_ernings":fs_factors[fs_fac_idx_b2015["이익잉여금"]],
        "full_equity":fs_factors[fs_fac_idx_b2015["자본총계"]]}
        )
    return res_fs_list

def get_financial_statements_factor_va2015(corp_code, fs_list):
    """extract financial statement factor from report issued after 2015
    """
    dbs = fs_list['bs']
    #bs_labels = fs_quarter.labels['bs'] # 라벨 정보
    dbs_columns = dbs.columns.to_flat_index()[7:]
    len_report = len(dbs_columns)
    res_fs_list = []
    for i in range(7, 7+len_report,1):
        fs_factors = dbs.iloc[:,i]
        res_fs_list.append(
        {"stock_code":corp_code,
        "year":int(dbs_columns[i-7][0][:4]),
        "quater":month2quater[int(dbs_columns[i-7][0][4:6])],
        "current_assets":fs_factors[fs_fac_idx_a2015["유동부채"]],
        "noncurrent_asset":fs_factors[fs_fac_idx_a2015["비유동부채"]],
        "full_assets":fs_factors[fs_fac_idx_a2015["자산총계"]],
        "current_liabilities":fs_factors[fs_fac_idx_a2015["유동부채"]],
        "noncurrent_liabilities":fs_factors[fs_fac_idx_a2015["비유동부채"]],
        "full_liabilities":fs_factors[fs_fac_idx_a2015["부채총계"]],
        "issued_capital":fs_factors[fs_fac_idx_a2015["자본금"]],
        "retain_ernings":fs_factors[fs_fac_idx_a2015["이익잉여금"]],
        "full_equity":fs_factors[fs_fac_idx_a2015["자본총계"]]}
        )
def get_year_and_interval(c_index):
    """
    분기 손익계산서 및 현금흐름표의 column이 아래와 같은 형식으로 되어있기 때문에
    누적 금액이 아니라 3개월간의 금액임을 확인하기 위해 해당 연(year), 분기(quarter), 3개월 금액인지
    확인하기 위한 정보를 column 으로 부터 출하는 함수
    ('20230401-20230630', ('연결재무제표',))
    """
    begin, end = c_index.split('-')
    year = int(begin[:4])
    begin_month = int(begin[4:6])
    # 분기 손익 정보인지 누적 정보인지 확인하기 위해
    gap = int(end[4:6])- int(begin[4:6]) + 1
    return year, begin_month, gap
    
def get_income_statements_factor(corp_code, fs_list):
    dis=fs_list['is']
    is_columns = dis.columns.to_flat_index()[7:]
    num_columns = len(is_columns)
    res_is_list = []
    for i in range(7, 7+num_columns,1):
        c_index = is_columns[i-7][0]
        year, begin_month, gap = get_year_and_interval(c_index)
        # 분기 손익 정보인지 누적 정보인지 확인하기 위해
        is_factor = dis.iloc[:,i]
        if gap==3 and not np.isnan(is_factor.loc[0]):
            res_is_list.append(
                {
                    "corp_code":corp_code,
                    "year": year,
                    "quarter":month2quater[begin_month],
                    "revenue":is_factor[is_fac_idx_b2015["매출"]],
                    "gross_progit":is_factor[is_fac_idx_b2015["매출총이익"]],
                    "operating_income":is_factor[is_fac_idx_b2015["영업이익"]],
                    "finance_cost": is_factor[is_fac_idx_b2015["금융비용"]],
                    "income_befor_tax":is_factor[is_fac_idx_b2015["법인세비용차감전순이익"]],
                    "net_profit":is_factor[is_fac_idx_b2015["당기순이익"]],
                    "diluted_eps":is_factor[is_fac_idx_b2015["희석주당이익"]]
                 }
            )
    return res_is_list
        

def get_cash_flow_statement_factor(corp_code, fs_list):
    pass