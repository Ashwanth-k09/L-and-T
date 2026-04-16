from fastmcp import Client
import asyncio

async def main():
    async with Client("http://10.185.180.230:8000/mcp") as client:
        tools = await client.list_tools()
        print("✅ Available tools:", [t.name for t in tools])

        print("\n=== AIRFLOW AGENT MENU ===")
        print("1. Install Airflow")
        print("2. Verify Airflow")
        print("3. Delete Airflow")
        choice = input("\nEnter choice: ")

        if choice == "1":
            port = int(input("Port (e.g. 8080): "))
            username = input("Username: ")
            password = input("Password: ")
            result = await client.call_tool("install_airflow", {
                "port": port,
                "username": username,
                "password": password
            })
            print(result.data)

        elif choice == "2":
            port = int(input("Port to verify: "))
            result = await client.call_tool("verify_airflow", {"port": port})
            print(result.data)

        elif choice == "3":
            result = await client.call_tool("delete_airflow", {})
            print(result.data)

asyncio.run(main())
