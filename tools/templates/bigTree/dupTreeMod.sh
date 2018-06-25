#!/bin/bash
# This file is to create a balanced binary tree project
TEMPLATE=$1
DEST=$2
NUM_MODS=$3

TBNAME=$(basename $TEMPLATE)
DUPNAME="$(echo $TBNAME | sed "s/\./0./")"
cp $TEMPLATE "$DEST/$DUPNAME"
perl -pi -e "s/_MOD_NAME_/M0/" "$DEST/$DUPNAME"
perl -pi -e "s/_PKG_NAME_/stage1.input/" "$DEST/$DUPNAME"
perl -pi -e "s/_DEP_NAME_/input/" "$DEST/$DUPNAME"

for((i=1;i<$NUM_MODS;i++)); do
LASTDUPNAME=$DUPNAME
DUPNAME="$(echo $TBNAME | sed "s/\./$i./")"
cp "$TEMPLATE" "$DEST/$DUPNAME"
perl -pi -e "s/_MOD_NAME_/M$i/" "$DEST/$DUPNAME"
VAR=i%2
if [ $VAR = 1 ]; then
  perl -pi -e "s/_PKG_NAME_/stage1.${TBNAME%.py}$(i/2)/" "$DEST/$DUPNAME"
  perl -pi -e "s/_DEP_NAME_/M$(i/2)/" "$DEST/$DUPNAME"
else
  perl -pi -e "s/_PKG_NAME_/stage1.${TBNAME%.py}$(((i-1)/2))/" "$DEST/$DUPNAME"
  perl -pi -e "s/_DEP_NAME_/M$(((i-1)/2))/" "$DEST/$DUPNAME"
fi
done
