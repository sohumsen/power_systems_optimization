import ftplib
import os
import re
import pandas as pd
"""
MIT license: 2017 
Example usage:
``` python
import ftplib
ftp = ftplib.FTP(mysite, username, password)
download_ftp_tree(ftp, remote_dir, local_dir)
```
The code above will look for a directory called "remote_dir" on the ftp host, and then duplicate the
directory and its entire contents into the "local_dir".
*** Note that if wget is an option, I recommend using that instead ***
"""

def clean_solar_excel_file(filename):
    
    df = pd.read_csv(filename, skiprows=75,
                            skipfooter=1, engine='python',parse_dates=True)
    df = df[["ob_end_time",'glbl_irad_amt']]
    df.fillna(0, inplace=True)
   
    df.to_csv(filename,index=False)
  
    return df

def clean_wind_excel_file(filename):
    
    df = pd.read_csv(filename, skiprows=280,
                            skipfooter=1, engine='python',parse_dates=True)
    df = df[["ob_time",'wind_speed']]
    df.fillna(0, inplace=True)
   
    df.to_csv(filename,index=False)
  
    return df
def _is_ftp_dir(ftp_handle, name, guess_by_extension=True):
    """ simply determines if an item listed on the ftp server is a valid directory or not """

    # if the name has a "." in the fourth to last position, its probably a file extension
    # this is MUCH faster than trying to set every file to a working directory, and will work 99% of time.
    if guess_by_extension is True:
        if len(name) >= 4:
            if name[-4] == '.':
                return False

    original_cwd = ftp_handle.pwd()  # remember the current working directory
    try:
        ftp_handle.cwd(name)  # try to set directory to new name
        ftp_handle.cwd(original_cwd)  # set it back to what it was
        return True

    except ftplib.error_perm as e:
        print(e)
        return False

    except Exception as e:
        print(e)
        return False


def _make_parent_dir(fpath):
    """ ensures the parent directory of a filepath exists """
    dirname = os.path.dirname(fpath)
    while not os.path.exists(dirname):
        try:
            os.makedirs(dirname)
            print("created {0}".format(dirname))
        except OSError as e:
            print(e)
            _make_parent_dir(dirname)


def _download_ftp_file(ftp_handle, name, dest, overwrite):
    """ downloads a single file from an ftp server """
 
    filename="/"+dest.split("/")[-1].split("_")[-1]
    destdir ='/'.join(dest.split("/")[:-1])
    folder_path=(destdir.split("/")[5])+"/"+(destdir.split("/")[6]).split("_")[-1]+filename
    _make_parent_dir(folder_path)

    if not os.path.exists(dest) or overwrite is True:
        try:
            
           
            local_path_of_csv=local_dir+folder_path
            print("local_dir",local_dir,"folder_path",folder_path)
            with open(local_path_of_csv, 'wb+') as f:
                
                ftp_handle.retrbinary("RETR {0}".format(name), f.write)

            clean_solar_excel_file(local_path_of_csv)
            print("downloaded: {0}".format(dest))
        except Exception as e:
            print("FAILED: {0}".format(dest),e)
    else:
        print("already exists: {0}".format(dest))


def _file_name_match_patern(pattern, name):
    """ returns True if filename matches the pattern"""
    if pattern is None:
        return True
    else:
        return bool(re.match(pattern, name))



def _mirror_ftp_dir(ftp_handle, name, overwrite, guess_by_extension, pattern):
    """ replicates a directory on an ftp server recursively """
    for item in ftp_handle.nlst(name):
        if _is_ftp_dir(ftp_handle, item, guess_by_extension):
            _mirror_ftp_dir(ftp_handle, item, overwrite, guess_by_extension, pattern)
        else:
            # print(name,item)
            if _file_name_match_patern(pattern, item):
                _download_ftp_file(ftp_handle, item, item, overwrite)
            else:
                # quietly skip the file
                pass
def download_ftp_tree(ftp_handle, path, destination, pattern=None, overwrite=False, guess_by_extension=True):
    """
    Downloads an entire directory tree from an ftp server to the local destination
    :param ftp_handle: an authenticated ftplib.FTP instance
    :param path: the folder on the ftp server to download
    :param destination: the local directory to store the copied folder
    :param pattern: Python regex pattern, only files that match this pattern will be downloaded.
    :param overwrite: set to True to force re-download of all files, even if they appear to exist already
    :param guess_by_extension: It takes a while to explicitly check if every item is a directory or a file.
        if this flag is set to True, it will assume any file ending with a three character extension ".???" is
        a file and not a directory. Set to False if some folders may have a "." in their names -4th position.
    """
    path = path.lstrip("/")
    original_directory = os.getcwd()  # remember working directory before function is executed
    os.chdir(destination)  # change working directory to ftp mirror directory
    print("ORIG",original_directory)
    _mirror_ftp_dir(
        ftp_handle,
        path,
        pattern=pattern,
        overwrite=overwrite,
        guess_by_extension=guess_by_extension)

    os.chdir(original_directory)  # reset working directory to what it was before function exec

if __name__ == "__main__":
    # Example usage mirroring all jpg files in an FTP directory tree.
    mysite = "ftp.ceda.ac.uk"
    username = os.environ['CEDA_USERNAME']
    password = os.environ['CEDA_PASSWORD']
    remote_dir = "/badc/ukmo-midas-open/data/uk-radiation-obs/dataset-version-202107/"
    local_dir = "C:/Sohum/Home/CodeHome/Python/optimization_course/energy_sytems_optimization/preprocessing/bulk_downloader/data/solar/"
    pattern = ".*\/qc-version-1\/.*(2015|2016|2017|2018|2019).csv"
    #pattern = ".*\/w.*\/.*\/qc-version-1\/.*(2015|2016|2017|2018|2019).csv"

    # pattern = ".*qc-version-1/"

    ftp = ftplib.FTP(mysite, username, password)
    download_ftp_tree(ftp, remote_dir, local_dir,pattern=pattern,overwrite=True, guess_by_extension=True)

    #clean_excel_file("preprocessing/bulk_downloader/data/badc/ukmo-midas-open/data/uk-hourly-weather-obs/dataset-version-202007/aberdeenshire//00159_fyvie-castle/qc-version-1/2015.csv")