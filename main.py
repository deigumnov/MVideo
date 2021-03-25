#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from http.server import SimpleHTTPRequestHandler
from itertools import islice
from json import dumps
import os
from socketserver import TCPServer
from urllib.parse import urlparse, parse_qs


def sort_condition(x: list) -> str:
    """This function is condition for sort data, now its sort by sku.
    x should not be empty.
        :param list x: list of data
        :return: first element in data
        :rtype: return type depends on data in list
    Examples:
        >>> lst = ['abc', 'def', 0.2]
        >>> sort_condition(lst)
        'abc'
    """
    return x[0]


def split_to_sorted_files(filename: str,
                          path_to_save: str,
                          read_buffer: int) -> list:
    """This function split big unsorted text file to many sorted files.
    Data in files sorted in ascending order.
        :param str filename: name of big file
        :param str path_to_save: OS path, where little files will be
         saved
        :param int read_buffer: Number of lines in one little file
        :return: it could return some exception cause of I/O and
         wrong path, but its not uses now
        :rtype: int
    Examples:
        >>> big_file = 'C:/data/bigfile.csv'
        >>> temp_path = 'C:/data/'
        >>> line_per_file = 10 ** 6
        >>> split_to_sorted_files(big_file, temp_path, line_per_file)
        Its return ['C:/data/tmp0.csv', .. ,'C:/data/tmpx.csv'] +
         create many files in C:/data/ with names like tmpXXX.csv.
         Directory C:/data/ and big file should be exist
    """
    file_list = []
    with open(filename, 'r', encoding='utf8') as in_file:
        file_number = 0
        while True:
            s = islice(in_file, 0, read_buffer)
            data = list(map(lambda x: x.split(','), list(s)))
            if not data:
                break
            out_file_name = f'{path_to_save}tmp{file_number}.csv'
            file_list.append(out_file_name)
            with open(out_file_name, 'w',
                      encoding='utf8',
                      newline='\n') as f:
                f.writelines([','.join(item)
                              for item in sorted(data,
                                                 key=sort_condition)])
            file_number += 1
    return file_list


def merge_files(filename: str, files: list) -> int:
    """This function merge sorted many files in a list to big sorted
     file. Algorithm description: open all files, find min string
     for conditions, add to result. Index k added to identify the file
     from which the line was added. It will remove files after work
        :param str filename:  Result sorted file name
        :param list files: List of sorted files
        :return: it could return some exception cause of I/O and
         wrong path, but its not uses now
        :rtype: int
    Examples:
        >>> result_filename = 'C:/data/result.csv'
        >>> little_files = ['C:/data/tmp1.csv', 'C:/data/tmp2.csv']
        >>> merge_files(result_filename, little_files)
        Its return 0 + create file C:/data/result.csv and remove
        files from param
    """
    with open(filename, 'w', encoding='utf8', newline='\n') as out:
        opened = [open(file, 'r', encoding='utf8') for file in files]
        lines_to_print = [
            file.readline().split(',') + [str(k)]
            for k, file in enumerate(opened)
        ]
        while True:
            if not lines_to_print:
                break
            min_line = min(lines_to_print, key=sort_condition)
            lines_to_print.remove(min_line)
            out.write(','.join(min_line[:3]))
            tmp = opened[int(min_line[3])].readline()
            if not tmp:
                continue
            lines_to_print += [tmp.split(',') + [min_line[3]]]
        for file in opened:
            file.close()
    for each in files:
        if os.path.isfile(each):
            os.remove(each)
    return 0


def create_index(sorted_file_name: str, index_file_name: str) -> int:
    """Create index file for faster search
        :param str sorted_file_name: Existed file with sorted data
        :param list index_file_name: This file will be created after
         work
        :return: it could return some exception cause of I/O and
         wrong path, but its not uses now
        :rtype: int
    Examples:
        >>> s_file_name = 'C:/data/sorted.csv'
        >>> i_file_name = 'C:/data/index.csv'
        >>> create_index(s_file_name, i_file_name)
        Its return 0 + create file C:/data/index.csv. With current algo
        file will be contain many strings like: sku,start,end where
        sku - searching data,
        start - first number of string in sorted file,
        end - last number number of string in sorted file.
        C:/data/sorted.csv should contain sorted data.
    """
    with open(sorted_file_name, 'r', encoding='utf8') as in_file:
        start = end = 0
        result = []
        sku = ''
        s = islice(in_file, 0, None)
        for i, line in enumerate(s):
            tmp_sku = line.split(',')[0]
            if tmp_sku != sku and i != 0:
                result.append([sku, str(start), str(end) + '\n'])
                start = i
            end, sku = i, tmp_sku
        result.append([sku, str(start), str(end) + '\n'])
        with open(index_file_name, 'w',
                  encoding='utf8',
                  newline='\n') as out:
            out.writelines([','.join(item) for item in result])
    return 0


def index_load(index_filename: str, dct: dict) -> int:
    """Loads index into dictionary
        :param str index_filename: Name of file with indexes
        :param dict dct: Dictionary for load indexes
        :return: it could return some exception cause of I/O and
         wrong path, but its not uses now
        :rtype: int
    Examples:
        >>> i_filename = 'C:/data/index.csv'
        >>> keys_dict = {}
        >>> index_load(i_filename, keys_dict)
        Its return 0 + load keys into keys_dict
    """
    with open(index_filename, 'r', encoding='utf8') as in_file:
        s = islice(in_file, 0, None)
        for line in s:
            tmp = line.rstrip().split(',')
            dct[tmp[0]] = {'start': int(tmp[1]), 'end': int(tmp[2])}
    return 0


def search_in_file(sorted_filename: str,
                   index_dct: dict,
                   sku: str,
                   rank=0.0) -> list:
    """This function will check sku in dict and return slice with
     values from sorted file with rank higher than in params. Algo
     based on fixed data size, so we could calculate offset from start
     number of string. Const value magic_str_length is justified by
     a fixed data size. For different data size its possible to create
     index with another start value, based on offset.
        :param str sorted_filename: Name of existed sorted file
        :param dict index_dct: Dict with keys & start + end lines
        :param str sku: Searching key
        :param float rank: Function will seerch ranks higher than this
        :return: it could return some exception cause of I/O and
         wrong path, but its not uses now
        :rtype: list of tupples with values and rank for this sku
    Examples:
        >>> s_filename = 'C:/data/result.csv'
        >>> dct = {'zzz80YZGlk':{'start': 1, 'end': 2}}
        >>> search_key = 'zzz80YZGlk'
        >>> min_rank = 0.5
        >>> search_in_file(s_filename, dct, search_key, min_rank)
        [('abracadbra', 0.6), ('myphonecad', 0.7)]
        return values based on abstract data in file
    """
    sku_recommends = []
    if sku in index_dct:
        sku_result = index_dct[sku]
        with open(sorted_filename, 'r', encoding='utf8') as in_file:
            magic_str_length = 26
            in_file.seek(magic_str_length * sku_result['start'])
            s = islice(in_file,
                       0,
                       sku_result['end'] - sku_result['start'] + 1)
            for item in s:
                tmp = item.rstrip().split(',')
                sku_recommends.append((tmp[1], float(tmp[2])))
    return sorted([i for i in sku_recommends if i[1] > rank],
                  key=lambda x: x[1])


class HttpRequestHandler(SimpleHTTPRequestHandler):
    def response(self, status) -> None:
        self.send_response(status)
        self.send_header('Content-type', 'application/json')
        self.end_headers()

    def do_GET(self) -> None:
        query = parse_qs(urlparse(self.path).query)
        query_sku = query['sku'][0] if 'sku' in query else ''
        query_rank = query['rank'][0] if 'rank' in query else 0.0
        try:
            query_rank = float(query_rank)
        except ValueError:
            query_rank = 0.0
        recommends = search_in_file(sorted_filename=sorted_file,
                                    index_dct=all_sku_dct,
                                    sku=query_sku,
                                    rank=query_rank)
        self.response(400) if not recommends else self.response(200)
        self.wfile.write(dumps(recommends).encode('utf-8'))


def run_server(port: int) -> None:
    object_handler = HttpRequestHandler
    server = TCPServer(("", port), object_handler)
    print(f'Server running on http://localhost:{port}')
    server.serve_forever()


if __name__ == '__main__':
    data_path = 'C:/Users/Dmitrii/Documents/Conda/' \
                'MyProjects/MVideo/data/'
    index_file = data_path + 'index.csv'
    sorted_file = data_path + 'result.csv'
    server_port = 8080
    '''
    ---------Steps split, merge, create not needed---------
    ------if you run it once without some I/O errors-------
    '''
    input_file = data_path + 'recommends.csv'
    lines_in_little_file = 1 * 10 ** 6
    out_files = split_to_sorted_files(filename=input_file,
                                      path_to_save=data_path,
                                      read_buffer=lines_in_little_file)
    merge_files(filename=sorted_file, files=out_files)
    create_index(sorted_file_name=sorted_file,
                 index_file_name=index_file)
    '''
    ----Preparation steps ended, you could comment them----
    ------if you run it once without some I/O errors-------
    '''
    all_sku_dct = {}
    index_load(index_filename=index_file,
               dct=all_sku_dct)

    run_server(server_port)
