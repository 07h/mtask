from setuptools import setup, find_packages

setup(
    name="μTask",
    version="0.1.0",
    description="A simple task queue and scheduler library using Redis and asyncio with greenlet-based workers.",
    author="Your Name",
    author_email="your.email@example.com",
    url="https://github.com/07h/mTask",
    packages=find_packages(),
    install_requires=[
        "aioredis>=2.0.0",
        "croniter>=1.0.15",
        "greenlet>=1.1.2",
    ],
    python_requires=">=3.8",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
)
