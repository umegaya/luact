CHECK=`luajit -v`
if [ "$CHECK" = "" ];
then 
pushd tmp
git clone http://luajit.org/git/luajit-2.0.git --branch $LUAJIT_VERSION
pushd luajit-2.0
make && sudo make install
popd
popd
fi
