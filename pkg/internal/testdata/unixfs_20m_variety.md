# unixfs_20m_variety (test fixture)

 * 20 MB of files with a variety of UnixFS features across 1,103 blocks
 * unixfs_20m_variety.car is a CARv1 in strict dfs order with a single root.

## Root CID
[testmark]:# (root)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq
```

## Test Cases

### Small file in directory

Same result regardless of scope.

#### all

[testmark]:# (test/small_file_in_directory/all/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/pi?dag-scope=all
```

[testmark]:# (test/small_file_in_directory/all/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafkreigtkfmisjmiqfp2y73lpqj7uu7mnqg7cjm5br67ek6nwsbyuqgkom | RawLeaf   | ↳ /pi[0:1701]
```

#### entity

[testmark]:# (test/small_file_in_directory/entity/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/pi?dag-scope=entity
```

[testmark]:# (test/small_file_in_directory/entity/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafkreigtkfmisjmiqfp2y73lpqj7uu7mnqg7cjm5br67ek6nwsbyuqgkom | RawLeaf   | ↳ /pi[0:1701]
```

#### block

[testmark]:# (test/small_file_in_directory/block/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/pi?dag-scope=block
```

[testmark]:# (test/small_file_in_directory/block/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafkreigtkfmisjmiqfp2y73lpqj7uu7mnqg7cjm5br67ek6nwsbyuqgkom | RawLeaf   | ↳ /pi[0:1701]
```

### Sharded file in directory

All and entity are the same but block should just get the root File block

#### all

[testmark]:# (test/sharded_file_in_directory/all/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/želva.xml?dag-scope=all
```

[testmark]:# (test/sharded_file_in_directory/all/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeidchppfcvpa644xihhbvbfyiqupsgp4efh4mf3pengqufwkdfrvha | File      | ↳ /želva.xml[0:1352050]
bafkreigtwsisgpg6x752y5md2z2r4jhuhohh46y4x3mvxrbkcubo7mqlgi | RawLeaf   |   ↳ /želva.xml[0:256144]
bafkreigahqispwg55yvqwobavwlheongcyhk63eufsaqutqgjiwfwesfau | RawLeaf   |     /želva.xml[256144:512288]
bafkreic4kgh44v2ung3wspd7y6wigcxake45ztpfx3c5ibfwmqe2kt7uay | RawLeaf   |     /želva.xml[512288:768432]
bafkreibdjsvoyftwgcywb3xylnfod2wififs2tj7pww4zkvaba7z5aigtm | RawLeaf   |     /želva.xml[768432:1024576]
bafkreidtv6frnbb4o4yobyu3xbtd5onzne67dhgngpd3vwrbdcneapy6fa | RawLeaf   |     /želva.xml[1024576:1280720]
bafkreidy2ntimx4u22b5uy6tjh7du5ex5lhgu7comxwcjfkzfbdsdunbou | RawLeaf   |     /želva.xml[1280720:1352050]
```

#### entity

[testmark]:# (test/sharded_file_in_directory/entity/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/želva.xml?dag-scope=entity
```

[testmark]:# (test/sharded_file_in_directory/entity/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeidchppfcvpa644xihhbvbfyiqupsgp4efh4mf3pengqufwkdfrvha | File      | ↳ /želva.xml[0:1352050]
bafkreigtwsisgpg6x752y5md2z2r4jhuhohh46y4x3mvxrbkcubo7mqlgi | RawLeaf   |   ↳ /želva.xml[0:256144]
bafkreigahqispwg55yvqwobavwlheongcyhk63eufsaqutqgjiwfwesfau | RawLeaf   |     /želva.xml[256144:512288]
bafkreic4kgh44v2ung3wspd7y6wigcxake45ztpfx3c5ibfwmqe2kt7uay | RawLeaf   |     /želva.xml[512288:768432]
bafkreibdjsvoyftwgcywb3xylnfod2wififs2tj7pww4zkvaba7z5aigtm | RawLeaf   |     /želva.xml[768432:1024576]
bafkreidtv6frnbb4o4yobyu3xbtd5onzne67dhgngpd3vwrbdcneapy6fa | RawLeaf   |     /želva.xml[1024576:1280720]
bafkreidy2ntimx4u22b5uy6tjh7du5ex5lhgu7comxwcjfkzfbdsdunbou | RawLeaf   |     /želva.xml[1280720:1352050]
```

#### block

[testmark]:# (test/sharded_file_in_directory/block/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/želva.xml?dag-scope=block
```

[testmark]:# (test/sharded_file_in_directory/block/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeidchppfcvpa644xihhbvbfyiqupsgp4efh4mf3pengqufwkdfrvha | File      | ↳ /želva.xml[0:1352050]
```

### Sharded file in directory in directory

Aame as above but one extra level of nesting.

#### all

[testmark]:# (test/sharded_file_in_directory_in_directory/all/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/Flibbertigibbet5/eorþscyld.pdf?dag-scope=all
```

[testmark]:# (test/sharded_file_in_directory_in_directory/all/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeicqlqnzd2tvs5vlo4c2dz72ryvouwmjwpu2mvxrocuj2u26joxeam | Directory | ↳ /Flibbertigibbet5
bafybeigutoywu5bj3hlcdr4mkm6yepqnwvq5xodulrz2wstwteko2266te | File      |   ↳ /eorþscyld.pdf[0:4558478]
bafkreidz265issniggrhdjbwp3c5o4fnmg7xsa6hbn3kmyo7bsafc3jjsy | RawLeaf   |     ↳ /eorþscyld.pdf[0:256144]
bafkreiepissrnjsgcanpe6hzen5kv66glcr5x5vw5vhferkjscxdj2uv3y | RawLeaf   |       /eorþscyld.pdf[256144:512288]
bafkreifohr4rlzjpskbdcntrjzolglq6e5uf7n4mgdiw7wl2yhovhx46ve | RawLeaf   |       /eorþscyld.pdf[512288:768432]
bafkreihiulihaonenbbtrbptmcmzcg2hvk6dypdxvmposz7ansd6h5mkla | RawLeaf   |       /eorþscyld.pdf[768432:1024576]
bafkreidbhmgdt3ajzgoeywwxdbdcquone7buyenwhirl6af2z3gmftiys4 | RawLeaf   |       /eorþscyld.pdf[1024576:1280720]
bafkreib6iawulrjvzet7dxbqdqneqa7kpy2kacvq7fawjvmlgveoakuzzy | RawLeaf   |       /eorþscyld.pdf[1280720:1536864]
bafkreicbbl7whtfqsoz6tu663qicdisdyvzp4pp4ylggcttc44j63bdhxy | RawLeaf   |       /eorþscyld.pdf[1536864:1793008]
bafkreifxe2irzoxtzk3ltpunioehodovu4pwzdtjr2lqtmkg7hyh3o5r2e | RawLeaf   |       /eorþscyld.pdf[1793008:2049152]
bafkreihr2m4zfx4qvpwkp2fnihn7gbjq6gyr2gv5nbibt2nuajnp2k6pvq | RawLeaf   |       /eorþscyld.pdf[2049152:2305296]
bafkreibhyumtv62kh3d4rvwpxkdlr52uur5thgmbtyuyxas3c424od7cta | RawLeaf   |       /eorþscyld.pdf[2305296:2561440]
bafkreiatyvemaol2uxceo4suktpmusl53dqgg5bcqi2rkfhlxnnvpigkci | RawLeaf   |       /eorþscyld.pdf[2561440:2817584]
bafkreias6ygzx2pnowi3hxj5kmed2rm74hgigwijiandz7vamvrohtkpje | RawLeaf   |       /eorþscyld.pdf[2817584:3073728]
bafkreihf5g2wmsblx67664w2k3m7hj2bg2wnpumlnty7ssw4gj46oj3bxa | RawLeaf   |       /eorþscyld.pdf[3073728:3329872]
bafkreib7gr6yi6lhl2p5izuxrxpknf5tc5lhkf5tdhzaxw5g66jxqichra | RawLeaf   |       /eorþscyld.pdf[3329872:3586016]
bafkreigmnnfehab7c4tblwqgydbaoc76y34rkjfetayb6ytql2eywqnq5y | RawLeaf   |       /eorþscyld.pdf[3586016:3842160]
bafkreiax5uhfktinmfo3ovm7geaf2266u5ya2slq4bob6eeginffbewszu | RawLeaf   |       /eorþscyld.pdf[3842160:4098304]
bafkreic5chuwrtofd6ymxawcaxulacfewzfi3zroakmkwjkfmadrxrmln4 | RawLeaf   |       /eorþscyld.pdf[4098304:4354448]
bafkreich7pcb4lnheypmqb4sikk5m6cuqqbxbjzuk767cx47djxac45o64 | RawLeaf   |       /eorþscyld.pdf[4354448:4558478]
```

#### entity

[testmark]:# (test/sharded_file_in_directory_in_directory/entity/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/Flibbertigibbet5/eorþscyld.pdf?dag-scope=entity
```

[testmark]:# (test/sharded_file_in_directory_in_directory/entity/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeicqlqnzd2tvs5vlo4c2dz72ryvouwmjwpu2mvxrocuj2u26joxeam | Directory | ↳ /Flibbertigibbet5
bafybeigutoywu5bj3hlcdr4mkm6yepqnwvq5xodulrz2wstwteko2266te | File      |   ↳ /eorþscyld.pdf[0:4558478]
bafkreidz265issniggrhdjbwp3c5o4fnmg7xsa6hbn3kmyo7bsafc3jjsy | RawLeaf   |     ↳ /eorþscyld.pdf[0:256144]
bafkreiepissrnjsgcanpe6hzen5kv66glcr5x5vw5vhferkjscxdj2uv3y | RawLeaf   |       /eorþscyld.pdf[256144:512288]
bafkreifohr4rlzjpskbdcntrjzolglq6e5uf7n4mgdiw7wl2yhovhx46ve | RawLeaf   |       /eorþscyld.pdf[512288:768432]
bafkreihiulihaonenbbtrbptmcmzcg2hvk6dypdxvmposz7ansd6h5mkla | RawLeaf   |       /eorþscyld.pdf[768432:1024576]
bafkreidbhmgdt3ajzgoeywwxdbdcquone7buyenwhirl6af2z3gmftiys4 | RawLeaf   |       /eorþscyld.pdf[1024576:1280720]
bafkreib6iawulrjvzet7dxbqdqneqa7kpy2kacvq7fawjvmlgveoakuzzy | RawLeaf   |       /eorþscyld.pdf[1280720:1536864]
bafkreicbbl7whtfqsoz6tu663qicdisdyvzp4pp4ylggcttc44j63bdhxy | RawLeaf   |       /eorþscyld.pdf[1536864:1793008]
bafkreifxe2irzoxtzk3ltpunioehodovu4pwzdtjr2lqtmkg7hyh3o5r2e | RawLeaf   |       /eorþscyld.pdf[1793008:2049152]
bafkreihr2m4zfx4qvpwkp2fnihn7gbjq6gyr2gv5nbibt2nuajnp2k6pvq | RawLeaf   |       /eorþscyld.pdf[2049152:2305296]
bafkreibhyumtv62kh3d4rvwpxkdlr52uur5thgmbtyuyxas3c424od7cta | RawLeaf   |       /eorþscyld.pdf[2305296:2561440]
bafkreiatyvemaol2uxceo4suktpmusl53dqgg5bcqi2rkfhlxnnvpigkci | RawLeaf   |       /eorþscyld.pdf[2561440:2817584]
bafkreias6ygzx2pnowi3hxj5kmed2rm74hgigwijiandz7vamvrohtkpje | RawLeaf   |       /eorþscyld.pdf[2817584:3073728]
bafkreihf5g2wmsblx67664w2k3m7hj2bg2wnpumlnty7ssw4gj46oj3bxa | RawLeaf   |       /eorþscyld.pdf[3073728:3329872]
bafkreib7gr6yi6lhl2p5izuxrxpknf5tc5lhkf5tdhzaxw5g66jxqichra | RawLeaf   |       /eorþscyld.pdf[3329872:3586016]
bafkreigmnnfehab7c4tblwqgydbaoc76y34rkjfetayb6ytql2eywqnq5y | RawLeaf   |       /eorþscyld.pdf[3586016:3842160]
bafkreiax5uhfktinmfo3ovm7geaf2266u5ya2slq4bob6eeginffbewszu | RawLeaf   |       /eorþscyld.pdf[3842160:4098304]
bafkreic5chuwrtofd6ymxawcaxulacfewzfi3zroakmkwjkfmadrxrmln4 | RawLeaf   |       /eorþscyld.pdf[4098304:4354448]
bafkreich7pcb4lnheypmqb4sikk5m6cuqqbxbjzuk767cx47djxac45o64 | RawLeaf   |       /eorþscyld.pdf[4354448:4558478]
```

#### block

[testmark]:# (test/sharded_file_in_directory_in_directory/block/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/Flibbertigibbet5/eorþscyld.pdf?dag-scope=block
```

[testmark]:# (test/sharded_file_in_directory_in_directory/block/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeicqlqnzd2tvs5vlo4c2dz72ryvouwmjwpu2mvxrocuj2u26joxeam | Directory | ↳ /Flibbertigibbet5
bafybeigutoywu5bj3hlcdr4mkm6yepqnwvq5xodulrz2wstwteko2266te | File      |   ↳ /eorþscyld.pdf[0:4558478]
```

### Sharded file in hamt in directory

Same as above but the inner directory is a HAMT and we have an intermediate block.

#### all

[testmark]:# (test/sharded_file_in_hamt_in_directory/all/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/O/Othello.png?dag-scope=all
```

[testmark]:# (test/sharded_file_in_hamt_in_directory/all/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeiahh6giyfzxpsp6b4y6j6r34xjgsb2si7n6dszwoevnjxdcuee5yq | HAMTShard | ↳ /O
bafybeics7ohtet4wfuzszfchbttrgotqzpuruq4bencpdtc443vawbcbni | HAMTShard |   ↳ <hamt>
bafybeigqgljdsq72owvi3mtgjwkrfmmjpevzbya6pu52ufj55h5ypznpx4 | File      |     ↳ /Othello.png[0:2977731]
bafkreibyblakon7atsv2hwzyzrweeb5uriksuscfxjtlzj3l56xmrouwke | RawLeaf   |       ↳ /Othello.png[0:256144]
bafkreic7nx6e7zezkdz5lp2xcwarmgavr62cyrmtepyagvm2ivuo3jesqe | RawLeaf   |         /Othello.png[256144:512288]
bafkreicylwcsi7ovreggex5yc7jolt5zrwf74ida3sprzsw37oxpzkwvte | RawLeaf   |         /Othello.png[512288:768432]
bafkreiainpsn6gadltlhqzgakxvdbxrz5n2cstizn3afhsr2nwefus7pqm | RawLeaf   |         /Othello.png[768432:1024576]
bafkreihuwjz4av7scz3fdjejtrx3k6ycdjudjusuz6sjzbbsewpgslosxi | RawLeaf   |         /Othello.png[1024576:1280720]
bafkreiexmzld3vi72v665bbqn473mfc5smmtd6nnndl7dzcapzlxdwzhq4 | RawLeaf   |         /Othello.png[1280720:1536864]
bafkreignp6prdhvi4mxoef5ybl4c644eyeikd42w3z2atw5mgbzw5p4374 | RawLeaf   |         /Othello.png[1536864:1793008]
bafkreia6nxplcal74ycukjwfmwmaaraeg6qwclfvb6qja2iaayukzsedoa | RawLeaf   |         /Othello.png[1793008:2049152]
bafkreiae6iay4fqiethqcvva4qazdxb7szb4dh77dsxiskvewhm5sepwfm | RawLeaf   |         /Othello.png[2049152:2305296]
bafkreieprkc6i3qzg47k5y3zxejsoglyl5fwrk3fnslbmdmau7surwiocy | RawLeaf   |         /Othello.png[2305296:2561440]
bafkreidwsenghahefqcfw5ikxd6osi6a35koyki4gibw3sunlmqrroqzc4 | RawLeaf   |         /Othello.png[2561440:2817584]
bafkreicn24jr4rwjcptuzu677u2aeqyirf7weradcafcnkq5hzo7wickkq | RawLeaf   |         /Othello.png[2817584:2977731]
```

#### entity

[testmark]:# (test/sharded_file_in_hamt_in_directory/entity/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/O/Othello.png?dag-scope=entity
```

[testmark]:# (test/sharded_file_in_hamt_in_directory/entity/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeiahh6giyfzxpsp6b4y6j6r34xjgsb2si7n6dszwoevnjxdcuee5yq | HAMTShard | ↳ /O
bafybeics7ohtet4wfuzszfchbttrgotqzpuruq4bencpdtc443vawbcbni | HAMTShard |   ↳ <hamt>
bafybeigqgljdsq72owvi3mtgjwkrfmmjpevzbya6pu52ufj55h5ypznpx4 | File      |     ↳ /Othello.png[0:2977731]
bafkreibyblakon7atsv2hwzyzrweeb5uriksuscfxjtlzj3l56xmrouwke | RawLeaf   |       ↳ /Othello.png[0:256144]
bafkreic7nx6e7zezkdz5lp2xcwarmgavr62cyrmtepyagvm2ivuo3jesqe | RawLeaf   |         /Othello.png[256144:512288]
bafkreicylwcsi7ovreggex5yc7jolt5zrwf74ida3sprzsw37oxpzkwvte | RawLeaf   |         /Othello.png[512288:768432]
bafkreiainpsn6gadltlhqzgakxvdbxrz5n2cstizn3afhsr2nwefus7pqm | RawLeaf   |         /Othello.png[768432:1024576]
bafkreihuwjz4av7scz3fdjejtrx3k6ycdjudjusuz6sjzbbsewpgslosxi | RawLeaf   |         /Othello.png[1024576:1280720]
bafkreiexmzld3vi72v665bbqn473mfc5smmtd6nnndl7dzcapzlxdwzhq4 | RawLeaf   |         /Othello.png[1280720:1536864]
bafkreignp6prdhvi4mxoef5ybl4c644eyeikd42w3z2atw5mgbzw5p4374 | RawLeaf   |         /Othello.png[1536864:1793008]
bafkreia6nxplcal74ycukjwfmwmaaraeg6qwclfvb6qja2iaayukzsedoa | RawLeaf   |         /Othello.png[1793008:2049152]
bafkreiae6iay4fqiethqcvva4qazdxb7szb4dh77dsxiskvewhm5sepwfm | RawLeaf   |         /Othello.png[2049152:2305296]
bafkreieprkc6i3qzg47k5y3zxejsoglyl5fwrk3fnslbmdmau7surwiocy | RawLeaf   |         /Othello.png[2305296:2561440]
bafkreidwsenghahefqcfw5ikxd6osi6a35koyki4gibw3sunlmqrroqzc4 | RawLeaf   |         /Othello.png[2561440:2817584]
bafkreicn24jr4rwjcptuzu677u2aeqyirf7weradcafcnkq5hzo7wickkq | RawLeaf   |         /Othello.png[2817584:2977731]
```

#### block

[testmark]:# (test/sharded_file_in_hamt_in_directory/block/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/O/Othello.png?dag-scope=block
```

[testmark]:# (test/sharded_file_in_hamt_in_directory/block/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeiahh6giyfzxpsp6b4y6j6r34xjgsb2si7n6dszwoevnjxdcuee5yq | HAMTShard | ↳ /O
bafybeics7ohtet4wfuzszfchbttrgotqzpuruq4bencpdtc443vawbcbni | HAMTShard |   ↳ <hamt>
bafybeigqgljdsq72owvi3mtgjwkrfmmjpevzbya6pu52ufj55h5ypznpx4 | File      |     ↳ /Othello.png[0:2977731]
```

### Sharded file in directory in hamt in directory

Same as above but with an extra directory layer under the HAMT.

#### all

[testmark]:# (test/sharded_file_in_directory_in_hamt_in_directory/all/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/Zigzagumptious/Throttlebottom/supercalifragilisticexpialidocious.txt?dag-scope=all
```

[testmark]:# (test/sharded_file_in_directory_in_hamt_in_directory/all/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeie3mbyp7k77vrpiduaifyhdeqvusjn7qofzwtsp47yavyyb62z32y | HAMTShard | ↳ /Zigzagumptious
bafybeied5si546vcp6klpngu77kftitzffdlxk5ajaytadc3p3ptr7tvam | HAMTShard |   ↳ <hamt>
bafybeifjkcls323ddq3t5ov22yl7i6ks36a7jzv33pjy4stqkils6jzvqe | Directory |     ↳ /Throttlebottom
bafybeify427noacqiu6sxaaunk5uw2xhelnkcwktkvh6woi6ahlipsz7em | File      |       ↳ /supercalifragilisticexpialidocious.txt[0:568521]
bafkreicdwgfhxwnzq7i34cuhnqqqxz6d6jtirupjy5kq3uaphopyw5e2ky | RawLeaf   |         ↳ /supercalifragilisticexpialidocious.txt[0:256144]
bafkreifmk6qfl6ucap7btfu35rjd37bh7uuefavq6nsuungxjl3or7bz2e | RawLeaf   |           /supercalifragilisticexpialidocious.txt[256144:512288]
bafkreifk2vcldftxe57ml2cxyfcwb34ukkhaopm46kv3as5vo26ht63fci | RawLeaf   |           /supercalifragilisticexpialidocious.txt[512288:568521]
```

#### entity

[testmark]:# (test/sharded_file_in_directory_in_hamt_in_directory/entity/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/Zigzagumptious/Throttlebottom?dag-scope=entity
```

[testmark]:# (test/sharded_file_in_directory_in_hamt_in_directory/entity/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeie3mbyp7k77vrpiduaifyhdeqvusjn7qofzwtsp47yavyyb62z32y | HAMTShard | ↳ /Zigzagumptious
bafybeied5si546vcp6klpngu77kftitzffdlxk5ajaytadc3p3ptr7tvam | HAMTShard |   ↳ <hamt>
bafybeifjkcls323ddq3t5ov22yl7i6ks36a7jzv33pjy4stqkils6jzvqe | Directory |     ↳ /Throttlebottom
```

#### block

[testmark]:# (test/sharded_file_in_directory_in_hamt_in_directory/block/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/Zigzagumptious/Throttlebottom?dag-scope=block
```

[testmark]:# (test/sharded_file_in_directory_in_hamt_in_directory/block/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeie3mbyp7k77vrpiduaifyhdeqvusjn7qofzwtsp47yavyyb62z32y | HAMTShard | ↳ /Zigzagumptious
bafybeied5si546vcp6klpngu77kftitzffdlxk5ajaytadc3p3ptr7tvam | HAMTShard |   ↳ <hamt>
bafybeifjkcls323ddq3t5ov22yl7i6ks36a7jzv33pjy4stqkils6jzvqe | Directory |     ↳ /Throttlebottom
```

### Hamt in directory

"All" is too large to be useful, so we'll just do "entity" and "block".

#### entity

[testmark]:# (test/hamt_in_directory/entity/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/Zigzagumptious?dag-scope=entity
```

[testmark]:# (test/hamt_in_directory/entity/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeie3mbyp7k77vrpiduaifyhdeqvusjn7qofzwtsp47yavyyb62z32y | HAMTShard | ↳ /Zigzagumptious
bafybeiggzqpxmfbve7sutrmcwrlwpx4tkno536d7qdgd6hnhxf74t7gzee | HAMTShard |   ↳ /Zigzagumptious
bafybeigfwz54atwgnklb72rfqqn2a3ywoontgf3b3kekxwoxalptx7gc3e | HAMTShard |     ↳ /Zigzagumptious
bafybeifr3w3eyq2oi6hxwbfoblor7s5pfvu3m4xyyzovxlk4g5xgslsqui | HAMTShard |     /Zigzagumptious
bafybeigivdbrxmcixaqfap3rrs7ti5ycr7vj4xfbkjougq67k7ac5oz55a | HAMTShard |     /Zigzagumptious
bafybeid3cny2p7fwt4znzm3wjncsesdk2rdze5fm5rzxjpbethy4657j6m | HAMTShard |     /Zigzagumptious
bafybeidivwpxv37o6p4ton3xcpmg64pwk4zqlkuge7isafgsgjsvb6ce2u | HAMTShard |     /Zigzagumptious
bafybeib5ptplpmtmoyif4lxt5erw5diivp6so4s57lvgfigs53d7zpc4uu | HAMTShard |     /Zigzagumptious
bafybeiafmairlfkvgqgticndcs4uomkll4h5xotvnnsw2j7cx3rjwrbh7i | HAMTShard |     ↳ /Zigzagumptious
bafybeifjo6xz3onvav6cnpjjuroa7vbzegoxgg5jn6smfvgfcpzwzdaipa | HAMTShard |     /Zigzagumptious
bafybeied5si546vcp6klpngu77kftitzffdlxk5ajaytadc3p3ptr7tvam | HAMTShard |     /Zigzagumptious
bafybeiar4b5hfriv2pmmbj7npemto6bev7sqkn4jpstwqtaudexpetpg4m | HAMTShard |     /Zigzagumptious
bafybeifcekpohkm7qaczfwrrzcdz6j3ykwntawdnp7b7fjfech75vxpwf4 | HAMTShard |     ↳ /Zigzagumptious
bafybeihnejxifhvlunhcswhghhrbues366h7o4xqp3ziyeicrdt5473za4 | HAMTShard |     /Zigzagumptious
bafybeihlijmc2xyfmomw4fx53mlz3gpt6svgfu3vfokm4wmyd4dagfup24 | HAMTShard |     /Zigzagumptious
bafybeihjbh7kptdfaoanrkzhdpx2jwjacg5ncdlcbbwqlum6vwqfga5fii | HAMTShard |     /Zigzagumptious
bafybeidaz5aob6tdurpnuj2xeqsjkarqudtono6x2kgbzrzv46ugp3g4ju | HAMTShard |     /Zigzagumptious
```

#### block

[testmark]:# (test/hamt_in_directory/block/query)
```
/ipfs/bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq/Zigzagumptious?dag-scope=block
```

[testmark]:# (test/hamt_in_directory/block/execution)
```
bafybeifrrglx2issn2had5rtstn3xltla6vxmpjfwfz7o3hapvkynh4zoq | Directory | /
bafybeie3mbyp7k77vrpiduaifyhdeqvusjn7qofzwtsp47yavyyb62z32y | HAMTShard | ↳ /Zigzagumptious
```
