import re

path='app.py'
with open(path,encoding='utf-8') as f:
    lines=f.readlines()

def indent_level(line):
    return len(line) - len(line.lstrip(' '))

problems = []
for i,l in enumerate(lines,1):
    m = re.match(r'^(\s*)try\s*:\s*(#.*)?$', l)
    if m:
        base_indent = len(m.group(1))
        found = False
        # search following lines for an except/finally at same indent
        for j in range(i+1, len(lines)+1):
            lj = lines[j-1]
            if lj.strip()=='' or lj.strip().startswith('#'):
                continue
            # if this line is less indented than the try block, the try block ended
            if indent_level(lj) <= base_indent:
                # check if this line starts with except/finally
                if re.match(r'^(\s*)(except|finally)\b', lj):
                    found = True
                break
            # if we find except/finally inside deeper indent, keep scanning
            if re.match(r'^(\s*)(except|finally)\b', lj):
                # found except at some indent (likely aligned), mark true
                # ensure it's at same base indent level
                if indent_level(lj) == base_indent:
                    found = True
                    break
        if not found:
            problems.append((i, 'try without matching except/finally'))

if problems:
    for ln,msg in problems:
        print(f'Problem at line {ln}: {msg}')
else:
    print('All try blocks appear to have except/finally at same indent')
