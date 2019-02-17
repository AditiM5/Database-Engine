#!/bin/bash
if [ "$1" = "main" ]; then
    sudo docker build -t dbi_project --build-arg make_target=main .
    sudo docker run -v /home/suraj/Projects/DBI-Project/data:/opt/build/data -it --cap-add=SYS_PTRACE dbi_project ./main
else
    sudo docker build -t dbi_project --build-arg make_target=test .
    sudo docker run -v /home/suraj/Projects/DBI-Project/data:/opt/build/data -it --cap-add=SYS_PTRACE dbi_project ./test.out
fi