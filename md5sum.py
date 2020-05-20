import sys
import hashlib
def md5sum(filename):
    file_object = open(filename, 'rb')
    file_content = file_object.read()
    file_object.close()
    file_md5 = hashlib.md5(file_content)
    return file_md5

if __name__ == "__main__":
    file_md5 = md5sum('text1')
    print(file_md5.hexdigest())