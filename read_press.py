# ...existing code...
from pathlib import Path
import pandas as pd

def read_press(path='/Users/sally/Desktop/Fall2025/NLP/Project_1/build_press_corpus/press_releases_sp500.csv'):
    p = Path(path)
    if not p.exists():
        alt = list(Path('.').glob('press_releases*.csv'))
        if alt:
            p = alt[0]
        else:
            print('⚠️ No press release CSV found. Skipping tariff sentiment...')
            return None
# ...existing

    # 首先检查CSV文件的列结构
    df = pd.read_csv(p)
    print(f"Original columns: {list(df.columns)}")
    
    df.columns = [c.lower() for c in df.columns]
    print(f"Lowercased columns: {list(df.columns)}")
    
    # 检查是否同时有id和ticker列
    has_id = 'id' in df.columns
    has_ticker = 'ticker' in df.columns
    
    if has_id and has_ticker:
        print("Both 'id' and 'ticker' columns found. Using 'ticker' column.")
        idc = 'ticker'
    elif has_ticker:
        idc = 'ticker'
    elif has_id:
        idc = 'id'
    else:
        # 如果既没有id也没有ticker，尝试其他标识符
        idc = next((c for c in ['symbol','permno'] if c in df.columns), None)
    
    txt = next((c for c in ['text','body','content'] if c in df.columns), None)
    dte = next((c for c in ['date','ann_date','timestamp'] if c in df.columns), None)
    
    print(f"Detected columns: identifier={idc}, text={txt}, date={dte}")
    
    if not all([idc, txt, dte]):
        print("Available columns that might contain identifier info:", [c for c in df.columns if 'tick' in c or 'symb' in c or 'id' in c])
        print("Available columns that might contain text info:", [c for c in df.columns if 'text' in c or 'body' in c or 'content' in c])
        print("Available columns that might contain date info:", [c for c in df.columns if 'date' in c or 'time' in c])
        raise ValueError('press release file must have identifier (ticker/id), date, and text columns')
    
    # 重命名为ticker列，无论原来是什么
    df = df.rename(columns={idc:'ticker', txt:'text', dte:'date'})
    df['date'] = pd.to_datetime(df['date']).dt.normalize()
    
    # 如果ticker列是NaN，尝试从text列中提取ticker
    if df['ticker'].isna().all():
        print("Ticker column is all NaN. Attempting to extract ticker from text column...")
        
        # 假设ticker在text的开头，以"-"分隔
        def extract_ticker(text):
            if pd.isna(text):
                return None
            # 提取第一个"-"之前的部分作为ticker
            parts = str(text).split('-')
            if len(parts) > 0:
                ticker = parts[0].strip().upper()
                # 验证ticker格式（通常3-4个字母）
                if len(ticker) >= 2 and len(ticker) <= 5 and ticker.isalpha():
                    return ticker
            return None
        
        df['ticker'] = df['text'].apply(extract_ticker)
        print(f"Extracted tickers. Sample: {df['ticker'].dropna().head(3).tolist()}")
    
    # 显示样本数据以确认
    print(f"Sample data after processing:")
    print(df[['ticker','date','text']].head(3))
    
    return df[['ticker','date','text']]
