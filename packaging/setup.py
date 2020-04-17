import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="mcsdbitw-pkg-MoChenSerey",
    version="0.0.1",
    author="Mo Chen Serey",
    author_email="mochenserey@gmail.com",
    description="Package for service uptime data analysis",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3.7',
)
