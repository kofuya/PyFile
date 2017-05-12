import sys
import getopt
import threading
import time
import queue
import os.path
from functools import cmp_to_key
from tempfile import TemporaryFile
from datetime import datetime

queueLock = threading.Condition()


class Config:
    stop_thread = False
    column_info_list = []  # 所有列信息，做为类变量
    delimeter = ' '  # 分隔符
    thread_count = 2  # 线程数


class Data:
    def __init__(self, lines, dest):
        self.lines = lines
        self.dest = dest


class MergeFiles:
    def __init__(self, src1, src2, dest):
        self.src1 = src1
        self.src2 = src2
        self.dest = dest

    def execute(self):
        src1 = self.src1
        src2 = self.src2
        dest = self.dest
        buf_size = 8192

        lines1 = src1.readlines(buf_size)
        lines2 = src2.readlines(buf_size)
        index1 = 0
        index2 = 0
        len1 = len(lines1)
        len2 = len(lines2)
        while lines1 and lines2:
            l1 = lines1[index1]
            l2 = lines2[index2]
            c = cmp_line(Line.parse(l1), Line.parse(l2))
            if c == 1:
                dest.writelines(l2)
                index2 += 1
                if index2 >= len2:
                    lines2 = src2.readlines(buf_size)
                    index2 = 0
                    len2 = len(lines2)
            elif c == -1:
                dest.writelines(l1)
                index1 += 1
                if index1 >= len1:
                    lines1 = src1.readlines(buf_size)
                    index1 = 0
                    len1 = len(lines1)
            else:
                dest.writelines(l1)
                dest.writelines(l2)
                index1 += 1
                if index1 >= len1:
                    lines1 = src1.readlines(buf_size)
                    index1 = 0
                    len1 = len(lines1)
                index2 += 1
                if index2 >= len2:
                    lines2 = src2.readlines(buf_size)
                    index2 = 0
                    len2 = len(lines2)
        if index1 < len1:
            while index1 < len1:
                dest.writelines(lines1[index1])
                index1 += 1
            lines1 = src1.readlines(buf_size)
            while lines1:
                for l1 in lines1:
                    dest.writelines(l1)
                lines1 = src1.readlines(buf_size)

        if index2 < len2:
            while index2 < len2:
                dest.writelines(lines2[index2])
                index2 += 1
            lines2 = src2.readlines(buf_size)
            while lines2:
                for l2 in lines2:
                    dest.writelines(l2)
                lines2 = src2.readlines(buf_size)
        src1.close()
        src2.close()
        dest.flush()
        dest.seek(0)


class Column:  # 列信息对象
    # 列下标 index = 0
    # 是否反序，默认正序 reverse = False
    # 列数据类型 String:0 Int:1 Float:2
    # data_type = 0

    def __init__(self, index, reverse, data_type):
        self.index = index
        self.reverse = reverse
        self.data_type = data_type

    @classmethod  # 类方法
    def parse(cls, s):
        reverse = False
        data_type = 0
        min_index = len(s)
        i = s.find('r')
        if i > -1:
            reverse = True
            if i < min_index:
                min_index = i
        i = s.find('i')
        if i > -1:
            data_type = 1
            if i < min_index:
                min_index = i
        i = s.find('f')
        if i > -1:
            data_type = 2
            if i < min_index:
                min_index = i
        index = int(s[0:min_index]) - 1
        return Column(index, reverse, data_type)


class Line:  # 行对象
    # 行文本
    # line = ''
    # 排序比较的列
    # columns = []

    def __init__(self, line, columns):
        self.line = line
        self.columns = columns

    @classmethod
    def parse(cls, line):
        all_columns = line.split(Config.delimeter)
        all_len = len(all_columns)
        columns = []
        for column in Config.column_info_list:
            index = column.index
            if index < all_len:
                columns.append(all_columns[index])
        return Line(line, columns)


def cmp_line(l1, l2):  # 比较函数
    len1 = len(l1.columns)
    len2 = len(l2.columns)
    for i in range(len1):
        column_info = Config.column_info_list[i]
        if i >= len2:
            return -1 if column_info.reverse else 1
        c1 = l1.columns[i]
        c2 = l2.columns[i]
        if column_info.data_type == 1:
            c1 = int(c1)
            c2 = int(c2)
        elif column_info.data_type == 2:
            c1 = float(c1)
            c2 = float(c2)
        if c1 > c2:
            return -1 if column_info.reverse else 1
        elif c1 < c2:
            return 1 if column_info.reverse else -1
            # len1 < len2
    return 0 if len1 == len2 else 1 if Config.column_info_list[len1].reverse else -1


def file_name(file):
    file_path = file.name
    return os.path.basename(file_path)


def usage():
    print(
        'sort.py -i <input_filename> -o <output_filename> [-d <delimeter>] [-c <columns>] [-s <size>] [-t <threadCount>]')
    print('-i 输入源文件名')
    print('-o 输出目标文件名，如果未指定，则结果覆盖到源文件')
    print('-d 可选项，文件文本行的列分隔符，默认是空格')
    print('-c 可选项，相关排序列信息，包括列号（从1开始，按出现顺序优先级排序）、数据类型（i：整数，f：浮点数，默认：字符串）、是否反序（r），默认按第一列字符串类型正序（升序）排序')
    print('-s 可选项，源文件分段最大行数，如果不指定则单线程执行，否则多线程执行排序)')
    print('-t 可选项，线程数，指定 - s参数时生效，默认值：2')


def main(argv):
    input_filename = ''
    output_filename = ''
    size = -1
    try:
        opts, args = getopt.getopt(argv, "hi:o:d:c:s:t:",
                                   ["ifile=", "ofile=", "delim=", "columns=", "size=", "thread="])
    except getopt.GetoptError:
        usage()
        sys.exit(2)
    if len(opts) == 0:
        usage()
        sys.exit(2)
    for opt, arg in opts:
        if opt == '-h':
            usage()
            sys.exit()
        elif opt in ("-i", "--ifile"):
            input_filename = arg
        elif opt in ("-o", "--ofile"):
            output_filename = arg
        elif opt == '-d':
            Config.delimeter = arg
        elif opt == '-c':
            tmp = arg.split(',')
            for s in tmp:
                Config.column_info_list.append(Column.parse(s))
        elif opt == '-s':
            size = int(arg)
        elif opt == '-t':
            Config.thread_count = int(arg)
        else:
            usage()
            sys.exit(2)
    if input_filename == '':
        usage()
        sys.exit(2)
    if output_filename == '':
        output_filename = input_filename
    if size == -1:
        sort_file(input_filename, output_filename)
    else:
        k_sort_file(input_filename, output_filename, size)


class SortThread(threading.Thread):
    def __init__(self, work_queue, index):
        threading.Thread.__init__(self)
        self.work_queue = work_queue
        self.name = 'SortThread' + str(index)

    def run(self):
        while not Config.stop_thread:
            queueLock.acquire()
            if not self.work_queue.empty():
                data = self.work_queue.get()
                queueLock.notify()
                queueLock.release()
                print('%s: 开始排序列表，记录数：%d' % (self.name, len(data.lines)))
                begin_time = datetime.utcnow()
                data.lines.sort(key=cmp_to_key(cmp_line))
                print('%s: 排序完成，耗时：%s' % (self.name, str(datetime.utcnow() - begin_time)))
                for line in data.lines:
                    data.dest.write(line.line)
                data.dest.flush()
                data.dest.seek(0)
                data.lines.clear()
            else:
                queueLock.release()


class MergeFileThread(threading.Thread):
    def __init__(self, work_queue, index):
        threading.Thread.__init__(self)
        self.work_queue = work_queue
        self.name = 'MergeFileThread' + str(index)

    def run(self):
        while not Config.stop_thread:
            queueLock.acquire()
            if not self.work_queue.empty():
                merge_files = self.work_queue.get()
                queueLock.notify()
                queueLock.release()
                print('%s: 开始归并临时文件：%s + %s' % (self.name, file_name(merge_files.src1), file_name(merge_files.src2)))
                begin_time = datetime.utcnow()
                merge_files.execute()
                print('%s: 归并文件完成，耗时：%s' % (self.name, str(datetime.utcnow() - begin_time)))
            else:
                queueLock.release()


def k_sort_file(input_filename, output_filename, size):
    src_file = open(input_filename)
    lines = []
    n = 0
    files = []
    sort_threads = []

    work_queue = queue.Queue(Config.thread_count + 1)
    tc = Config.thread_count
    while tc > 0:
        t = SortThread(work_queue, tc)
        sort_threads.append(t)
        t.start()
        tc -= 1
    while True:
        buf = src_file.readlines(size * 32)
        if not buf:
            break
        for line in buf:
            if line.strip():
                if not line.endswith('\n'):
                    line += '\n'
                lines.append(Line.parse(line))
                n += 1
                if n == size:
                    f = TemporaryFile('w+t')
                    files.append(f)
                    queueLock.acquire()
                    while work_queue.full():
                        queueLock.wait()
                    work_queue.put(Data(lines[:], f))
                    queueLock.release()
                    n = 0
                    lines.clear()
    src_file.close()
    if len(lines) > 0:
        f = TemporaryFile('w+t')
        files.append(f)
        queueLock.acquire()
        while work_queue.full():
            queueLock.wait()
        work_queue.put(Data(lines[:], f))
        queueLock.release()

    # 等待任务队列全部完成
    while not work_queue.empty():
        pass

    # 通知线程退出循环
    Config.stop_thread = True

    # 等待所有线程完成
    for t1 in sort_threads:
        t1.join()

    # 文件归并排序
    files_size = len(files)
    count = 1
    while files_size > 0:
        if files_size == 2:
            f1 = files[0]
            f2 = files[1]
            f3 = open(output_filename, "w")
            print('===开始最终文件归并：%s + %s ---> %s' % (file_name(f1), file_name(f2), f3.name))
            begin_time = datetime.utcnow()
            MergeFiles(f1, f2, f3).execute()
            print('===最终文件归并结束,耗时：%s' % str(datetime.utcnow() - begin_time))
            f3.close()
            break
        else:
            print('===开始第%d轮文件归并===' % count)
            Config.stop_thread = False
            merge_threads = []
            tc = min(files_size, Config.thread_count)
            while tc > 0:
                t = MergeFileThread(work_queue, tc)
                merge_threads.append(t)
                t.start()
                tc -= 1
            new_files = []
            ii = 1
            while ii < files_size:
                f1 = files[ii - 1]
                f2 = files[ii]
                f3 = TemporaryFile('w+t')
                new_files.append(f3)
                queueLock.acquire()
                while work_queue.full():
                    queueLock.wait()
                work_queue.put(MergeFiles(f1, f2, f3))
                queueLock.release()
                ii += 2
            if ii == files_size:
                new_files.append(files[ii - 1])
            files.clear()
            files = new_files
            files_size = len(new_files)

            # 等待任务队列全部完成
            while not work_queue.empty():
                pass
            # 通知线程退出循环
            Config.stop_thread = True
            # 等待所有线程完成
            for t1 in merge_threads:
                t1.join()
            print('===第%d轮文件归并结束===' % count)
            count += 1


def sort_file(input_filename, output_filename):
    lines = []
    for line in open(input_filename):
        if not line.endswith('\n'):
            line += '\n'
        lines.append(Line.parse(line))

    # 排序
    lines.sort(key=cmp_to_key(cmp_line))
    # 保存
    dest_file = open(output_filename, "w")
    for line in lines:
        dest_file.write(line.line)

    # 关闭文件
    dest_file.close()


if __name__ == "__main__":
    start_time = time.time()
    main(sys.argv[1:])
    print('总耗时：%d 秒' % (time.time() - start_time))
