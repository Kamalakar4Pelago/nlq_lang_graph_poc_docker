from setuptools import setup, find_packages

setup(
    name="lang_graph_poc",
    version="0.1",
    packages=find_packages(),
    install_requires=[
        "langchain-core>=0.1.0",
        "langchain-openai>=0.0.5",
        "langgraph>=0.0.10",
        "psycopg2-binary>=2.9.9",
        "pandas>=2.1.0",
        "python-dotenv>=1.0.0",
        "streamlit>=1.31.0",
    ],
)