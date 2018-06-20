#!/bin/bash
TEMPLATE=$1
DEST=$2
NUM_MODS=$3

TBNAME=$(basename $TEMPLATE)
DUPNAME="$(echo $TBNAME | sed "s/\./0./")"
cp $TEMPLATE "$DEST/$DUPNAME"
sed -i "" "s/_MOD_NAME_/M0/" "$DEST/$DUPNAME"
sed -i "" "s/_PKG_NAME_/stage1.input/" "$DEST/$DUPNAME"
sed -i "" "s/_DEP_NAME_/input/" "$DEST/$DUPNAME"

for((i=1;i<$NUM_MODS;i++)); do
LASTDUPNAME=$DUPNAME
DUPNAME="$(echo $TBNAME | sed "s/\./$i./")"
cp "$TEMPLATE" "$DEST/$DUPNAME"
sed -i "" "s/_MOD_NAME_/M$i/" "$DEST/$DUPNAME"
VAR=i%2
if [ $VAR = 1 ]; then
  sed -i "" "s/_PKG_NAME_/stage1.${TBNAME%.py}$(((i-1)/2))/" "$DEST/$DUPNAME"
  sed -i "" "s/_DEP_NAME_/M$(((i-1)/2))/" "$DEST/$DUPNAME"
else
  sed -i "" "s/_PKG_NAME_/stage1.${TBNAME%.py}$(((i-2)/2))/" "$DEST/$DUPNAME"
  sed -i "" "s/_DEP_NAME_/M$(((i-2)/2))/" "$DEST/$DUPNAME"
fi
done
