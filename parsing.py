import re
from bs4 import BeautifulSoup
import polars as pl


class ETFSiseExtractor:
    """
    HTML 소스에서 테이블 데이터를 추출하고 정제하는 클래스 (Polars 사용)
    """
    def __init__(self, source: str):
        """
        ETFSiseExtractor 초기화
        
        Parameters:
            source (str): HTML 페이지 소스
        """
        self.soup = BeautifulSoup(source, 'html.parser')

    def extract_table(self, table_index=0) -> pl.DataFrame:
        """
        지정된 인덱스의 테이블을 추출하고 Polars DataFrame으로 반환
        
        Parameters:
            table_index (int): 대상 테이블의 인덱스 (기본값 0)
            
        Returns:
            pl.DataFrame: 정제된 테이블 데이터
        """
        tables = self.soup.find_all('table')

        if not tables or table_index >= len(tables):
            raise ValueError("해당하는 테이블이 존재하지 않습니다.")
        
        table = tables[table_index]
        headers = self._get_headers(table)
        data = self._get_data(table)
        return self._to_dataframe(data, headers)
    
    def _get_headers(self, table):
        """테이블의 헤더를 추출"""
        header_row = table.find('tr')
        header_row =[th.text.strip() for th in header_row.find_all('th')] if header_row else []
        header_row.insert(1, 'code')
        return header_row
    
    def _get_data(self, table):
        """테이블의 데이터를 추출"""
        rows = table.find_all('tr')[1:]
        data = []
        for row in rows:
            cols = row.find_all('td')
            row_data = []
            for col in cols:
                a_tag = col.find('a')
                if a_tag and 'href' in a_tag.attrs:
                    href = a_tag['href']
                    code = self._extract_code_from_href(href)
                    row_data.append(a_tag.text.strip())  
                    row_data.append(code) 
                else:
                    row_data.append(col.text.strip() if col.text.strip() else None)
            if any(row_data):  
                data.append(row_data)
        return data

    def _extract_code_from_href(self, href):
        """href에서 code 값을 추출"""
        match = re.search(r'code=(\d+)', href)
        return match.group(1) if match else None

    def _to_dataframe(self, data, headers):
        """데이터와 헤더를 기반으로 Polars DataFrame 생성"""
        df = pl.DataFrame(data, schema=headers if headers else None)
        df = df.drop_nulls() 
        return df