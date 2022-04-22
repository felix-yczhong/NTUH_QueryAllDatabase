import setuptools

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setuptools.setup(
    name="NTUH_QueryAllDatabase",
    version="0.0.1dev",
    author="Yu-Chang Zhong",
    author_email="r09922111@ntu.edu.tw",
    description="A small tool to automate Mviewer query all database function in batch",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/felix-yczhong/NTUH_QueryAllDatabase.git",
    # package_dir={"": "NTUH_QueryAllDatabase"},
    # packages=setuptools.find_packages(where="NTUH_QueryAllDatabase"),
    # packages=setuptools.find_packages(),
    python_requires=">=3.6",
    include_package_data=True,
    entry_points={'console_scripts': [
                    'queryalldatabase=NTUH_QueryAllDatabase.main:main']}  
)