@echo off
magick mogrify -verbose -resize 256 -quality 75 covers/*.jpg
pause