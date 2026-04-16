from runner import run_command

def analyze_error(error_text: str):
    safe_text = error_text[-4000:].replace('"', '\\"')
    cmd = (
        "bash -lc 'command -v ollama >/dev/null 2>&1 || exit 99; "
        "ollama list >/dev/null 2>&1 || exit 98; "
        f"printf \"%s\" \"Analyze this Airflow installation error. Return probable cause, likely rule name, and whether retry is safe. Do not invent commands.\n\n{safe_text}\" | ollama run qwen2.5:3b'"
    )
    return run_command(cmd)
