
#kudos https://datascienceatthecommandline.com/2e/chapter-6-project-management-with-make.html?q=makefile#overview-3


.ONESHELL:  #do not call line by line, but entire target
#.SHELLFLAGS := -eu -o pipefail -c #stop pipeline in case of error

start=158000
interval=1000
finish=220000
bucket=gs://gdliveproject_htmls/

run-to: archive

complete_rids:
	bq query --format csv --max_rows 10000 --use_legacy_sql=false "SELECT DISTINCT recipient_id FROM gdliveproject.tests.recipients WHERE completed = True" > $@
	# default output limit is 100, needed to be set to 10000. Also bq uses legacy sql by default, which does not support DISTINCT

urls: SHELL := /usr/bin/bash
urls: complete_rids
	for i in `seq $(start) $(interval) $(finish)`; do
		if ! grep -q $$i complete_rids; then #if the rid is not found in the list of completed profiles, then add the url
			echo https://live.givedirectly.org/newsfeed/ec2f00f0-adf7-40b0-b592-954e552e4d90/$$i >> $@
		fi
	done

html: urls #Kudos to https://www.baeldung.com/linux/wget-parallel-downloading
	-cat urls | xargs --verbose --max-args 1 --max-procs 10 wget --content-on-error -P $@/ 
	#"-" in the beginning tells make to continue to next target even in case of errors in this line. Otherwise, xargs would raise an error when wget returns 404.

archive: html
	mkdir $@ && tar -czf $@/`date +%Y%m`.tar.gz html/* --remove-files 

.PHONY: upload
upload: archive
	gsutil cp archive/`date +%Y%m`.tar.gz $(bucket)

nuke: upload
	rm urls complete_rids -r html -r archive