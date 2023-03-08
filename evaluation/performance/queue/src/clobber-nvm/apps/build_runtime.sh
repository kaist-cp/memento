
cd runtime 
rm *.o
gcc -c context.c -o context.o
gcc -c wrapper.c -o wrapper.o
gcc -c clobber.c -o clobber.o
gcc -c pmdk.c -o pmdk.o
gcc -c nolog.c -o nolog.o
cd ..
