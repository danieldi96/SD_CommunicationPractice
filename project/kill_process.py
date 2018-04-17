import os

os.system("kill -9 $(ps -a|grep python|cut -f2 -d ' ')")
