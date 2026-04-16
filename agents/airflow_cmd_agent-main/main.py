from getpass import getpass

from config import DEFAULT_PORT, MAX_RETRIES
from checks import run_prechecks, check_airflow_installed, missing_packages
from installer import install_missing_apt, ensure_postgres, create_venv, install_airflow
from configurator import configure_airflow, start_airflow
from fixer import detect_fix, apply_fix
from verifier import verify_airflow
from ai_analyzer import analyze_error


def print_result(title, result):
    print(f"\n--- {title} ---")
    print("CMD:", result.get("cmd", ""))
    if result.get("stdout"):
        print(result["stdout"])
    if result.get("stderr"):
        print(result["stderr"])


def combine_error(result):
    return (result.get("stdout", "") or "") + "\n" + (result.get("stderr", "") or "")


def main():
    print("\n=== AIRFLOW CMD AGENT ===\n")

    port = input(f"Enter Airflow port [{DEFAULT_PORT}]: ").strip() or DEFAULT_PORT
    username = input("Enter admin username: ").strip()
    password = getpass("Enter admin password: ").strip()

    print("\nChecking current system...")
    for item in run_prechecks():
        print_result("Precheck", item)

    installed, version = check_airflow_installed()
    if installed:
        print(f"\nAirflow already detected: {version}")
        choice = input("Choose [1=verify, 2=repair, 3=reinstall]: ").strip() or "1"
        if choice == "1":
            ok, results = verify_airflow(port)
            for r in results:
                print_result("Verify", r)
            if ok:
                print("\nAirflow verification passed.")
            else:
                print("\nAirflow verification failed.")
            return

    pkgs = missing_packages()
    if pkgs:
        print("\nMissing packages found:")
        print(" ".join(pkgs))
        res = install_missing_apt(pkgs)
        print_result("Install missing apt packages", res)
        if res["returncode"] != 0:
            return
    else:
        print("\nNo missing apt packages.")

    res = ensure_postgres()
    print_result("Ensure PostgreSQL", res)
    if res["returncode"] != 0:
        return

    res = create_venv()
    print_result("Create virtualenv", res)
    if res["returncode"] != 0:
        return

    install_result = install_airflow()
    print_result("Install Airflow", install_result)

    retry = 0
    while install_result["returncode"] != 0 and retry < MAX_RETRIES:
        error_text = combine_error(install_result)
        fix_name, message = detect_fix(error_text)

        if not fix_name:
            print("\nNo known rule matched. Trying AI analysis...")
            ai_res = analyze_error(error_text)
            print_result("AI analysis", ai_res)
            return

        print(f"\nKnown install issue detected: {message}")
        fix_res = apply_fix(fix_name)
        print_result("Apply install fix", fix_res)

        install_result = install_airflow()
        print_result("Retry Airflow install", install_result)
        retry += 1

    if install_result["returncode"] != 0:
        print("\nInstallation failed after retries.")
        ai_res = analyze_error(combine_error(install_result))
        print_result("AI analysis", ai_res)
        return

    res = configure_airflow(username, password)
    print_result("Configure Airflow", res)

    config_retry = 0
    while res["returncode"] != 0 and config_retry < MAX_RETRIES:
        error_text = combine_error(res)
        fix_name, message = detect_fix(error_text)

        if not fix_name:
            print("\nNo known rule matched during configuration. Trying AI analysis...")
            ai_res = analyze_error(error_text)
            print_result("AI analysis", ai_res)
            return

        print(f"\nKnown configuration issue detected: {message}")
        fix_res = apply_fix(fix_name)
        print_result("Apply config fix", fix_res)

        res = configure_airflow(username, password)
        print_result("Retry Configure Airflow", res)
        config_retry += 1

    if res["returncode"] != 0:
        print("\nConfiguration failed after retries.")
        ai_res = analyze_error(combine_error(res))
        print_result("AI analysis", ai_res)
        return

    res = start_airflow(port)
    print_result("Start Airflow", res)
    if res["returncode"] != 0:
        return

    print("\nRunning verification...")
    ok, results = verify_airflow(port)
    for r in results:
        print_result("Verify", r)

    if ok:
        print(f"\nSUCCESS: Airflow agent finished. Webserver should be on port {port}.")
    else:
        print("\nAirflow installed, but runtime verification failed.")
        print("Check ~/airflow/webserver.log, ~/airflow/scheduler.log, ~/airflow/triggerer.log")


if __name__ == "__main__":
    main()
