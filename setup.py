"""Setup script for Conductor."""

from setuptools import setup, find_packages

with open("README.md", encoding="utf-8") as f:
    long_description = f.read()

with open("LICENSE", encoding="utf-8") as f:
    license_text = f.read()

setup(
    name="conductor-task-queue",
    version="0.1.0",
    description="Lightweight async task queue for Python (PostgreSQL-backed, no Redis)",
    long_description=long_description,
    long_description_content_type="text/markdown",
    author="Panagiotis Panageas",
    author_email="",
    url="https://github.com/Archangel-77/Conductor",
    license="MIT",
    license_files=("LICENSE",),
    packages=find_packages(exclude=["tests*", "examples*", "docs*"]),
    python_requires=">=3.11",
    install_requires=[
        "asyncpg>=0.29.0",
        "aiohttp>=3.9.0",
        "pydantic>=2.0.0",
        "prometheus-client>=0.19.0",
        "python-dotenv>=1.0.0",
    ],
    extras_require={
        "dev": [
            "pytest>=8.0.0",
            "pytest-asyncio>=0.23.0",
            "pytest-cov>=4.1.0",
            "black>=24.0.0",
            "mypy>=1.8.0",
            "flake8>=7.0.0",
        ],
    },
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3.11",
        "Programming Language :: Python :: 3.12",
        "Topic :: System :: Distributed Computing",
        "Topic :: Software Development :: Libraries :: Python Modules",
    ],
    keywords="task queue async postgresql worker background jobs",
    project_urls={
        "Bug Tracker": "https://github.com/Archangel-77/Conductor/issues",
        "Source Code": "https://github.com/Archangel-77/Conductor",
    },
)
