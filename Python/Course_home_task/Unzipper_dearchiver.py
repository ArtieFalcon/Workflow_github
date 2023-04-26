import os
import shutil
import zipfile


def unpack_zipfile(filename, extract_dir, encoding='cp437'):
    with zipfile.ZipFile(filename) as archive:
        for entry in archive.infolist():
            name = entry.filename.encode('cp437').decode(encoding)  # reencode!!!

            # don't extract absolute paths or ones with .. in them
            if name.startswith('/') or '..' in name:
                continue

            target = os.path.join(extract_dir, *name.split('/'))
            os.makedirs(os.path.dirname(target), exist_ok=True)
            if not entry.is_dir():  # file
                with archive.open(entry) as source, open(target, 'wb') as dest:
                    shutil.copyfileobj(source, dest)


current_dir = str(os.getcwd())  # Определяем директорию для сохранения
# print(current_dir)

for i in os.listdir(current_dir):
    if '.zip' in i:
        file_zip = i
unpack_zipfile(str(current_dir + '/' + file_zip), current_dir, encoding='cp866')
print(file_zip + ' распакован в ' + current_dir)