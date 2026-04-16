# src/analyzer.py
import pandas as pd
import json, os

def load_file(filepath):
    ext = os.path.splitext(filepath)[-1].lower()
    if ext == ".csv":            return pd.read_csv(filepath)
    elif ext == ".json":         return pd.read_json(filepath)
    elif ext == ".parquet":      return pd.read_parquet(filepath)
    elif ext in (".xlsx",".xls"):return pd.read_excel(filepath)
    else: raise ValueError(f"Unsupported: {ext}")

def analyze_dataset(filepath):
    df = load_file(filepath)
    schema = []
    for col in df.columns:
        schema.append({
            "name":     col,
            "dtype":    str(df[col].dtype),
            "nulls":    int(df[col].isnull().sum()),
            "null_pct": round(df[col].isnull().mean()*100, 2),
            "unique":   int(df[col].nunique()),
            "sample":   [str(v) for v in df[col].dropna().head(3).tolist()]
        })
    total_nulls = int(df.isnull().sum().sum())
    total_dupes = int(df.duplicated().sum())
    issues = []
    if total_nulls > 0:
        issues.append(f"{total_nulls} null values across {df.isnull().any().sum()} columns")
    if total_dupes > 0:
        issues.append(f"{total_dupes} duplicate rows")
    for col in df.columns:
        if df[col].isnull().mean() > 0.5:
            issues.append(f"Column '{col}' has >{50}% nulls — consider dropping")
    null_penalty  = (total_nulls / max(df.shape[0]*df.shape[1],1)) * 50
    dupe_penalty  = (total_dupes / max(len(df),1)) * 30
    quality_score = max(0, round(100 - null_penalty - dupe_penalty, 1))
    return {
        "file":          os.path.basename(filepath),
        "rows":          len(df),
        "columns":       len(df.columns),
        "memory_mb":     round(df.memory_usage(deep=True).sum()/1024/1024, 2),
        "schema":        schema,
        "issues":        issues or ["No major issues found ✅"],
        "quality_score": quality_score,
        "column_names":  list(df.columns),
        "dtypes":        {c: str(df[c].dtype) for c in df.columns},
        "preview":       df.head(5).to_dict(orient="records")
    }
