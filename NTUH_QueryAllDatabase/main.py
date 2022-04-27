import argparse
import datetime
import pathlib
import subprocess
import concurrent.futures
import os
import enum

ref = 'hg38'
date_of_query = datetime.date.today().strftime("%Y%m%d")

Mviewer_loc = pathlib.Path('C:\\Users\\User\\Desktop\\MViewer_20220426\\MViewer\\MViewer.exe')

mviewer_postfix = f'.{ref}_multianno.Q{date_of_query}_cli.mviewer.tsv'

class run_mode(enum.IntEnum):
    solo = 0
    trio = 1

def config_argparse():
    parser = argparse.ArgumentParser(description='call query all database in batch')
    parser.add_argument('path', type=str, help='path to target directory')
    parser.add_argument('-ref', choices=['hg19', 'hg38'], default='hg38')
    parser.add_argument('--ncpus', type=int, default=os.cpu_count())
    args = parser.parse_args()
    return args

def find_caseName_and_mode(path):
    solo_postfix = f'.{ref}_multianno.txt'
    trio_postfix = f'_A.{ref}_multianno.txt'

    for inner_entry in os.scandir(path / '7.ANNOVAR'):
        if inner_entry.is_file():
            if inner_entry.name.endswith(trio_postfix):
                mode = run_mode.trio
                case_name = inner_entry.name[:-len(trio_postfix)]
                break
            elif inner_entry.name.endswith(solo_postfix):
                mode = run_mode.solo
                case_name = inner_entry.name[:-len(solo_postfix)]
                break
    else:
        raise FileNotFoundError('cannot identify case name')
    return case_name, mode

def worker(command):
    args = command.split(' ')
    p = subprocess.run(args, shell=True, check=False)
    return p.returncode

def main():
    args = config_argparse()
    print(args)

    solo_info = {
        'vep': {'dir_name': '5.VEP', 'postfix': '.vep.txt'},
        'nirvana': {'dir_name': '8.Nirvana', 'postfix': '.json.gz'},
        'input': {'dir_name': '7.ANNOVAR', 'postfix': f'.{ref}_multianno.txt'},
        'intervar': {'dir_name': '16.InterVar', 'postfix': f'.{ref}_multianno.txt.intervar'},
        'expansionHunter': {'dir_name': '21.ExpansionHunter', 'postfix': '.HomRegions.tsv'},
        'automap': {'dir_name': '20.AutoMap', 'postfix': '.vcf'}
    }

    trio_info = {
        'vep': {'dir_name': '5.VEP', 'postfix': '.vep.txt'},
        'nirvana': {'dir_name': '8.Nirvana', 'postfix': '.json.gz'},
        'input': {'dir_name': '7.ANNOVAR', 'postfix': f'_A.{ref}_multianno.txt'},
        'father': {'dir_name': '7.ANNOVAR', 'postfix': f'_B.{ref}_multianno.txt'},
        'mother': {'dir_name': '7.ANNOVAR', 'postfix': f'_C.{ref}_multianno.txt'},
        'intervar': {'dir_name': '16.InterVar', 'postfix': f'.{ref}_multianno.txt.intervar'},
        'expansionHunter': {'dir_name': '21.ExpansionHunter', 'postfix': '.HomRegions.tsv'},
        'automap': {'dir_name': '20.AutoMap', 'postfix': '.vcf'}
    }
    
    path = pathlib.Path(os.path.abspath(args.path))

    if not path.exists():
        raise FileNotFoundError(f'{path} does not exist!')

    with concurrent.futures.ThreadPoolExecutor(args.ncpus) as executor:
        futures = dict()
        for outer_entry in os.scandir(path):
            if outer_entry.is_dir() and outer_entry.name not in ['done', 'novaseq_out', 'OUTPUT', 'reads'] and not outer_entry.name.startswith('.'):
                entries = os.listdir(outer_entry.path)
                if len(entries) != 1:
                    raise Exception(f'zero or more than 1 subfolder in {outer_entry.path}')

                sub_path = pathlib.Path(outer_entry.path) / entries[0]
                case_name, mode = find_caseName_and_mode(sub_path)

                info = solo_info if mode == run_mode.solo else trio_info
                param_dict = dict()
                for arg, d in info.items():
                    param_dict[arg] = sub_path / d['dir_name'] / f"{case_name}{d['postfix']}"
                param_dict['output'] = sub_path / f"{case_name}{mviewer_postfix}"

                folders = os.listdir(sub_path / '20.AutoMap')
                if len(folders) != 1:
                    raise Exception(f'zero or more than 1 subfolders in {sub_path / "20.AutoMap"}')
                param_dict['automap'] = path / info['automap']['dir_name'] / folders[0] / f"{case_name}{info['automap']['postfix']}"

                if mode == run_mode.solo:
                    MviewerCommand = \
                        (f"start /min /wait {Mviewer_loc} "
                        f"--ref {ref} " 
                        f"--input {param_dict['input']} "
                        f"--output {param_dict['output']} "
                        f"--nirvana {param_dict['nirvana']} "
                        f"--vep {param_dict['vep']} "
                        f"--intervar {param_dict['intervar']} "
                        f"--automap {param_dict['automap']} "
                        f"--expansionHunter {param_dict['expansionHunter']} "
                        "--always-dump")
                elif mode == run_mode.trio:
                    MviewerCommand = \
                        (f"{Mviewer_loc} "
                        f"--ref {ref} " 
                        f"--input {param_dict['input']} "
                        f"--output {param_dict['output']} "
                        f"--nirvana {param_dict['nirvana']} "
                        f"--vep {param_dict['vep']} "
                        f"--intervar {param_dict['intervar']} "
                        f"--automap {param_dict['automap']} "
                        f"--expansionHunter {param_dict['expansionHunter']} "
                        f"--father {param_dict['father']} "
                        f"--mother {param_dict['mother']} "
                        "--always-dump")
                future = executor.submit(worker, command=MviewerCommand)
                futures[future] = case_name

        for future in concurrent.futures.as_completed(futures):
            returnCode = future.result()
            case_name = futures[future]
            print(f"returnCode {returnCode}")

if __name__ == '__main__': 
    main()