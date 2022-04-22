import argparse
import datetime
import pathlib
import concurrent
import os

ref = 'hg38'
date_of_query = datetime.date.today().strftime("%Y%m%d")

Mviewer_loc = ''

dirs = ['5.VEP', '7.ANNOVAR', '8.Nirvana', '16.InterVar', '20.AutoMap', '21.ExtensionHunter']
postfixes = ['.vep.txt', f'.{ref}_multianno.txt', '.json.gz', f'.{ref}_multianno.txt.intervar', '.vcf', '.HomRegions.tsv']
Postfixes = {d: p for d, p in zip(dirs, postfixes)}

mviewer_name = f'.{ref}_multianno.Q{date_of_query}_cli.mviewer.tsv'

key = '7.ANNOVAR'

def config_argparse():
    parser = argparse.ArgumentParser(description='call query all database in batch')
    parser.add_argument('path', type=str, help='path to target directory')
    parser.add_argument('-ref', choices=['hg19', 'hg38'], default='hg38')
    args = parser.parse_args()
    return args

def find_file(path, dir, postfix):
    candidates = list()
    for entry in os.scandir(path / dir):
        if entry.is_file() and entry.name.endswith(postfix):
            candidates.append(entry)
    if len(candidates) == 0:
        raise FileNotFoundError(f'cannot find file in {dir.split(".")[1]} search')
    if len(candidates) > 1:
        raise Exception(f'More than one possible files are found during {dir.split(".")[1]} search')
    return candidates[0]

def worker(command):
    os.system(command)

def main():
    args = config_argparse()
    print(args)
    
    path = pathlib.Path(os.path.abspath(args.path))

    if not path.exists():
        raise FileNotFoundError(f'{path} does not exist!')

    candidates = list()
    for entry in os.scandir(path / '7.ANNOVAR'):
        if entry.is_file() and entry.name.endswith(Postfixes['7.ANNOVAR']):
            candidates.append(entry)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        fs = dict()
        for candidate in candidates:
            case_name = candidate[:len(Postfixes('7.ANNOVAR'))]

            paths = dict()
            for key, value in Postfixes.items():
                paths[key] = path / key / f'{case_name}{value}'
            paths['output'] = path / f'{case_name}{mviewer_name}'
            
            MviewerCommand = \
                (f"{Mviewer_loc} "
                f"--ref {ref}" 
                f"--input {paths['7.ANNOVAR']} "
                f"--output {paths['output']} "
                f"--nirvana {paths['8.Nirvana']} "
                f"--vep {paths['5.VEP']} "
                f"--intervar {paths['16.InterVar']} "
                f"--automap {paths['20.AutoMap']} "
                f"--expansionHunter {paths['21.ExtensionHunter']} "
                "--always-dump")
            # f = executor.submit(worker, command=MviewerCommand)
            # fs[f] = case_name
        for f in concurrent.futures.as_completed(fs):
            if f.result() != 0:
                print(f'{fs[f]} failure!')

    # for outer_entry in os.scandir(path):
    #     if outer_entry.is_dir():
    #         inner_entry = find_file(pathlib.Path(outer_entry.path), key, Postfixes[key])
    #         case_name = (inner_entry.name)[:-len(Postfixes[key])]

    #         param_dict = {}
    #         for dir, postfix in zip(dirs, postfixes):
    #             entry = find_file(pathlib.Path(outer_entry.path), dir, postfix)
    #             param_dict[dir] = entry.path

    #         MviewerCommand = \
    #         (f"{Mviewer_loc} "
    #         f"--ref {ref}" 
    #         f"--input {param_dict['7.ANNOVAR']} "
    #         f"--output {path} "
    #         f"--nirvana {param_dict['8.Nirvana']} "
    #         f"--vep {param_dict['5.VEP']} "
    #         f"--intervar {param_dict['16.InterVar']} "
    #         f"--automap {param_dict['20.AutoMap']} "
    #         f"--expansionHunter {param_dict['21.ExtensionHunter']} "
    #         "--always-dump")

    #         print(MviewerCommand)

if __name__ == '__main__':
    main()