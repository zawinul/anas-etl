|operation|priority|par1|par2|par3|extra|can push|descr|
|---|---|---|---|---|---|---|---|
|getFolderMD|1000|os|path|-|{<br>maxrecursion,<br>withdoc,<br>withcontent<br>}|getFolderMD<br>getFolderDocs|dato un path estrae i metadati del folder|
|getFolderDocs|900|os|path|-|{<br>withcontent<br>}|getDocMd|aaaa|
|getDocMD|800|os|docId|-|{<br>withcontent,<br>path<br>}|getContent|aaa|
|getContent|700|os|docId|_|{<br>path<br>}|_|bbb|
