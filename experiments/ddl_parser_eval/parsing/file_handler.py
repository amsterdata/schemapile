from chardet.universaldetector import UniversalDetector
import os
import uuid
import json
import codecs

class FileHandler:
    def __init__(self, file_path):
        self.OUTPATH = './out/filedetails/'

        self.file_path = file_path
        self.file_id = file_path.split('/')[-1].split('_')[0]
        self.sql_file = None
        self.encoding = {}
        self.num_lines = None
        self.file_size_bytes = None
        self.file_contents = None
        self.err_replace = 0

    def file_open(self, file_encoding=None):
        if file_encoding is None:
            self.sql_file = open(self.file_path, 'rb')
        else:
            try:
                self.sql_file = open(self.file_path,'r',encoding=file_encoding)
            except ValueError:
                #print("Error in file open() operation for: " + self.file_path)
                self.sql_file = open(self.file_path,'r',encoding=file_encoding, errors='replace')
                self.err_replace = 1

    def file_close(self):
        self.sql_file.close()

    def file_read(self):
        try:
            self.file_contents = self.sql_file.read()
        except Exception as e:
            # TODO remove
            #print("================")
            #print(e)
            #print("Error in file read() operation for: " + self.file_path)

            # have to re-open file
            self.file_close()
            self.sql_file = open(self.file_path,'rb')
            self.file_contents = self.sql_file.read().decode(errors='replace')
            self.err_replace = 1
            #print(len(self.file_contents))
            #print("================")

        self.sql_file.seek(0)

    def file_stats(self):
        self.file_size_bytes = os.path.getsize(self.file_path)
        self.num_lines = 0
        self.char_count = 0
        for line in self.sql_file:
            self.num_lines +=1
            for _ in line:
                self.char_count +=1
        
        self.sql_file.seek(0)

    def file_encoding(self):
        detector = UniversalDetector()
        detector.reset()
        for line in self.sql_file:
            detector.feed(line)
            if detector.done: break
        detector.close()
        self.encoding["charset"] = detector.result['encoding']
        self.encoding["confidence"] = detector.result['confidence']

    def get_encoding(self):
        return self.encoding["charset"], self.encoding["confidence"]

    def get_file_details(self):
        self.file_open()
        self.file_read()
        self.file_stats()
        self.file_encoding()
        self.file_close()

    def write_to_json(self, outdir, data):
        out_file_path = outdir + str(uuid.uuid4()) + '.json'

        with open(out_file_path, 'w+', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def output(self):
        data = {
            'file_id': self.file_id,
            'file_path': self.file_path,
            'file_size': self.file_size_bytes,
            'line_nr': self.num_lines,
            'char_nr': self.char_count,
            'encoding': self.encoding['charset'],
            'encoding_confidence': self.encoding['confidence']
        }
        self.write_to_json(self.OUTPATH, data)

    def get_data(self):
        data = {
            'file_id': self.file_id,
            'file_path': self.file_path,
            'file_size': self.file_size_bytes,
            'line_nr': self.num_lines,
            'char_nr': self.char_count,
            'encoding': self.encoding['charset'],
            'encoding_confidence': self.encoding['confidence']
        }
        return self.OUTPATH, data