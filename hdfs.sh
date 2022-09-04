###########Processing###########

#How is data structured?
[----nz@mathmadslinux2p ~]$ hdfs dfs -ls /data/ghcnd/
# Found 6 items
# drwxr-xr-x   - jsw93 supergroup          0 2022-03-16 10:45 /data/ghcnd/daily
# -rw-r--r--   8 jsw93 supergroup       3659 2021-08-23 09:29 /data/ghcnd/ghcnd-countries.txt
# -rw-r--r--   8 jsw93 supergroup   32428298 2021-08-23 09:29 /data/ghcnd/ghcnd-inventory.txt
# -rw-r--r--   8 jsw93 supergroup       1086 2021-08-23 09:29 /data/ghcnd/ghcnd-states.txt
# -rw-r--r--   8 jsw93 supergroup   10190398 2021-08-23 09:29 /data/ghcnd/ghcnd-stations.txt
# -rw-r--r--   8 jsw93 supergroup      26612 2021-08-09 14:56 /data/ghcnd/readme.txt

#How many years are contained in daily, and how does the size of the data change?
[----nz@mathmadslinux2p ~]$ hdfs dfs -ls /data/ghcnd/daily/
# Found 260 items
# -rw-r--r--   8 jsw93 supergroup       3358 2021-08-09 15:08 /data/ghcnd/daily/1763.csv.gz
# -rw-r--r--   8 jsw93 supergroup       3327 2021-08-09 15:03 /data/ghcnd/daily/1764.csv.gz
# -rw-r--r--   8 jsw93 supergroup       3335 2021-08-09 15:03 /data/ghcnd/daily/1765.csv.gz
# -rw-r--r--   8 jsw93 supergroup       3344 2021-08-09 14:56 /data/ghcnd/daily/1766.csv.gz
# -rw-r--r--   8 jsw93 supergroup       3356 2021-08-09 15:06 /data/ghcnd/daily/1767.csv.gz
# -rw-r--r--   8 jsw93 supergroup       3325 2021-08-09 15:02 /data/ghcnd/daily/1768.csv.gz
# -rw-r--r--   8 jsw93 supergroup       3418 2021-08-09 15:03 /data/ghcnd/daily/1769.csv.gz
# -rw-r--r--   8 jsw93 supergroup       3357 2021-08-09 15:07 /data/ghcnd/daily/1770.csv.gz
# -rw-r--r--   8 jsw93 supergroup       3373 2021-08-09 15:06 /data/ghcnd/daily/1771.csv.gz
# -rw-r--r--   8 jsw93 supergroup       3419 2021-08-09 15:05 /data/ghcnd/daily/1772.csv.gz
# -rw-r--r--   8 jsw93 supergroup       3368 2021-08-09 15:08 /data/ghcnd/daily/1773.csv.gz
# -rw-r--r--   8 jsw93 supergroup       3393 2021-08-09 15:03 /data/ghcnd/daily/1774.csv.gz
# -rw-r--r--   8 jsw93 supergroup       6435 2021-08-09 14:59 /data/ghcnd/daily/1775.csv.gz
# -rw-r--r--   8 jsw93 supergroup       6502 2021-08-09 15:06 /data/ghcnd/daily/1776.csv.gz
# -rw-r--r--   8 jsw93 supergroup       6432 2021-08-09 15:06 /data/ghcnd/daily/1777.csv.gz
# -rw-r--r--   8 jsw93 supergroup       6233 2021-08-09 14:57 /data/ghcnd/daily/1778.csv.gz
# -rw-r--r--   8 jsw93 supergroup       6158 2021-08-09 15:03 /data/ghcnd/daily/1779.csv.gz
# -rw-r--r--   8 jsw93 supergroup       6277 2021-08-09 14:57 /data/ghcnd/daily/1780.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7729 2021-08-09 15:00 /data/ghcnd/daily/1781.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7770 2021-08-09 15:00 /data/ghcnd/daily/1782.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7838 2021-08-09 15:07 /data/ghcnd/daily/1783.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7929 2021-08-09 15:00 /data/ghcnd/daily/1784.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7822 2021-08-09 15:07 /data/ghcnd/daily/1785.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7857 2021-08-09 14:59 /data/ghcnd/daily/1786.csv.gz
# -rw-r--r--   8 jsw93 supergroup       6339 2021-08-09 15:01 /data/ghcnd/daily/1787.csv.gz
# -rw-r--r--   8 jsw93 supergroup       6424 2021-08-09 15:03 /data/ghcnd/daily/1788.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7751 2021-08-09 15:08 /data/ghcnd/daily/1789.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7702 2021-08-09 15:01 /data/ghcnd/daily/1790.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7718 2021-08-09 15:01 /data/ghcnd/daily/1791.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7754 2021-08-09 15:00 /data/ghcnd/daily/1792.csv.gz
# -rw-r--r--   8 jsw93 supergroup       6322 2021-08-09 15:03 /data/ghcnd/daily/1793.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7677 2021-08-09 14:57 /data/ghcnd/daily/1794.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7766 2021-08-09 15:05 /data/ghcnd/daily/1795.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7773 2021-08-09 14:57 /data/ghcnd/daily/1796.csv.gz
# -rw-r--r--   8 jsw93 supergroup       9174 2021-08-09 14:57 /data/ghcnd/daily/1797.csv.gz
# -rw-r--r--   8 jsw93 supergroup       9116 2021-08-09 14:57 /data/ghcnd/daily/1798.csv.gz
# -rw-r--r--   8 jsw93 supergroup       6329 2021-08-09 14:57 /data/ghcnd/daily/1799.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7727 2021-08-09 14:58 /data/ghcnd/daily/1800.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7752 2021-08-09 14:57 /data/ghcnd/daily/1801.csv.gz
# -rw-r--r--   8 jsw93 supergroup       9123 2021-08-09 15:07 /data/ghcnd/daily/1802.csv.gz
# -rw-r--r--   8 jsw93 supergroup       7858 2021-08-09 15:04 /data/ghcnd/daily/1803.csv.gz
# -rw-r--r--   8 jsw93 supergroup       8709 2021-08-09 15:02 /data/ghcnd/daily/1804.csv.gz
# -rw-r--r--   8 jsw93 supergroup       9023 2021-08-09 15:08 /data/ghcnd/daily/1805.csv.gz
# -rw-r--r--   8 jsw93 supergroup       8639 2021-08-09 15:00 /data/ghcnd/daily/1806.csv.gz
# -rw-r--r--   8 jsw93 supergroup       8731 2021-08-09 15:02 /data/ghcnd/daily/1807.csv.gz
# -rw-r--r--   8 jsw93 supergroup       8875 2021-08-09 14:57 /data/ghcnd/daily/1808.csv.gz
# -rw-r--r--   8 jsw93 supergroup       8736 2021-08-09 14:59 /data/ghcnd/daily/1809.csv.gz
# -rw-r--r--   8 jsw93 supergroup       8765 2021-08-09 15:00 /data/ghcnd/daily/1810.csv.gz
# -rw-r--r--   8 jsw93 supergroup       8790 2021-08-09 15:03 /data/ghcnd/daily/1811.csv.gz
# -rw-r--r--   8 jsw93 supergroup       8858 2021-08-09 15:07 /data/ghcnd/daily/1812.csv.gz
# -rw-r--r--   8 jsw93 supergroup       9393 2021-08-09 14:57 /data/ghcnd/daily/1813.csv.gz
# -rw-r--r--   8 jsw93 supergroup      12140 2021-08-09 15:07 /data/ghcnd/daily/1814.csv.gz
# -rw-r--r--   8 jsw93 supergroup      15282 2021-08-09 14:59 /data/ghcnd/daily/1815.csv.gz
# -rw-r--r--   8 jsw93 supergroup      15186 2021-08-09 15:06 /data/ghcnd/daily/1816.csv.gz
# -rw-r--r--   8 jsw93 supergroup      15064 2021-08-09 15:02 /data/ghcnd/daily/1817.csv.gz
# -rw-r--r--   8 jsw93 supergroup      15096 2021-08-09 14:57 /data/ghcnd/daily/1818.csv.gz
# -rw-r--r--   8 jsw93 supergroup      15100 2021-08-09 15:08 /data/ghcnd/daily/1819.csv.gz
# -rw-r--r--   8 jsw93 supergroup      15368 2021-08-09 14:56 /data/ghcnd/daily/1820.csv.gz
# -rw-r--r--   8 jsw93 supergroup      15183 2021-08-09 15:02 /data/ghcnd/daily/1821.csv.gz
# -rw-r--r--   8 jsw93 supergroup      15366 2021-08-09 15:01 /data/ghcnd/daily/1822.csv.gz
# -rw-r--r--   8 jsw93 supergroup      16178 2021-08-09 14:56 /data/ghcnd/daily/1823.csv.gz
# -rw-r--r--   8 jsw93 supergroup      19566 2021-08-09 14:58 /data/ghcnd/daily/1824.csv.gz
# -rw-r--r--   8 jsw93 supergroup      19706 2021-08-09 14:56 /data/ghcnd/daily/1825.csv.gz
# -rw-r--r--   8 jsw93 supergroup      19982 2021-08-09 15:03 /data/ghcnd/daily/1826.csv.gz
# -rw-r--r--   8 jsw93 supergroup      22600 2021-08-09 15:00 /data/ghcnd/daily/1827.csv.gz
# -rw-r--r--   8 jsw93 supergroup      22597 2021-08-09 14:57 /data/ghcnd/daily/1828.csv.gz
# -rw-r--r--   8 jsw93 supergroup      22850 2021-08-09 15:06 /data/ghcnd/daily/1829.csv.gz
# -rw-r--r--   8 jsw93 supergroup      23039 2021-08-09 15:02 /data/ghcnd/daily/1830.csv.gz
# -rw-r--r--   8 jsw93 supergroup      22981 2021-08-09 15:01 /data/ghcnd/daily/1831.csv.gz
# -rw-r--r--   8 jsw93 supergroup      24099 2021-08-09 15:00 /data/ghcnd/daily/1832.csv.gz
# -rw-r--r--   8 jsw93 supergroup      28718 2021-08-09 15:05 /data/ghcnd/daily/1833.csv.gz
# -rw-r--r--   8 jsw93 supergroup      28559 2021-08-09 14:57 /data/ghcnd/daily/1834.csv.gz
# -rw-r--r--   8 jsw93 supergroup      29203 2021-08-09 15:02 /data/ghcnd/daily/1835.csv.gz
# -rw-r--r--   8 jsw93 supergroup      30066 2021-08-09 14:58 /data/ghcnd/daily/1836.csv.gz
# -rw-r--r--   8 jsw93 supergroup      29663 2021-08-09 15:01 /data/ghcnd/daily/1837.csv.gz
# -rw-r--r--   8 jsw93 supergroup      32159 2021-08-09 15:02 /data/ghcnd/daily/1838.csv.gz
# -rw-r--r--   8 jsw93 supergroup      29761 2021-08-09 15:03 /data/ghcnd/daily/1839.csv.gz
# -rw-r--r--   8 jsw93 supergroup      34807 2021-08-09 14:57 /data/ghcnd/daily/1840.csv.gz
# -rw-r--r--   8 jsw93 supergroup      35950 2021-08-09 15:00 /data/ghcnd/daily/1841.csv.gz
# -rw-r--r--   8 jsw93 supergroup      38642 2021-08-09 15:00 /data/ghcnd/daily/1842.csv.gz
# -rw-r--r--   8 jsw93 supergroup      38428 2021-08-09 14:57 /data/ghcnd/daily/1843.csv.gz
# -rw-r--r--   8 jsw93 supergroup      42257 2021-08-09 15:03 /data/ghcnd/daily/1844.csv.gz
# -rw-r--r--   8 jsw93 supergroup      46156 2021-08-09 15:06 /data/ghcnd/daily/1845.csv.gz
# -rw-r--r--   8 jsw93 supergroup      43510 2021-08-09 15:03 /data/ghcnd/daily/1846.csv.gz
# -rw-r--r--   8 jsw93 supergroup      45014 2021-08-09 15:08 /data/ghcnd/daily/1847.csv.gz
# -rw-r--r--   8 jsw93 supergroup      43801 2021-08-09 15:03 /data/ghcnd/daily/1848.csv.gz
# -rw-r--r--   8 jsw93 supergroup      46100 2021-08-09 14:57 /data/ghcnd/daily/1849.csv.gz
# -rw-r--r--   8 jsw93 supergroup      46162 2021-08-09 15:07 /data/ghcnd/daily/1850.csv.gz
# -rw-r--r--   8 jsw93 supergroup      53942 2021-08-09 14:56 /data/ghcnd/daily/1851.csv.gz
# -rw-r--r--   8 jsw93 supergroup      58274 2021-08-09 15:02 /data/ghcnd/daily/1852.csv.gz
# -rw-r--r--   8 jsw93 supergroup      57673 2021-08-09 14:58 /data/ghcnd/daily/1853.csv.gz
# -rw-r--r--   8 jsw93 supergroup      56548 2021-08-09 15:05 /data/ghcnd/daily/1854.csv.gz
# -rw-r--r--   8 jsw93 supergroup      60272 2021-08-09 15:02 /data/ghcnd/daily/1855.csv.gz
# -rw-r--r--   8 jsw93 supergroup      72670 2021-08-09 15:05 /data/ghcnd/daily/1856.csv.gz
# -rw-r--r--   8 jsw93 supergroup      78635 2021-08-09 15:05 /data/ghcnd/daily/1857.csv.gz
# -rw-r--r--   8 jsw93 supergroup     112312 2021-08-09 14:57 /data/ghcnd/daily/1858.csv.gz
# -rw-r--r--   8 jsw93 supergroup     118703 2021-08-09 15:07 /data/ghcnd/daily/1859.csv.gz
# -rw-r--r--   8 jsw93 supergroup     125540 2021-08-09 14:56 /data/ghcnd/daily/1860.csv.gz
# -rw-r--r--   8 jsw93 supergroup     129755 2021-08-09 14:57 /data/ghcnd/daily/1861.csv.gz
# -rw-r--r--   8 jsw93 supergroup     125723 2021-08-09 15:00 /data/ghcnd/daily/1862.csv.gz
# -rw-r--r--   8 jsw93 supergroup     141287 2021-08-09 14:56 /data/ghcnd/daily/1863.csv.gz
# -rw-r--r--   8 jsw93 supergroup     139789 2021-08-09 15:03 /data/ghcnd/daily/1864.csv.gz
# -rw-r--r--   8 jsw93 supergroup     145257 2021-08-09 15:07 /data/ghcnd/daily/1865.csv.gz
# -rw-r--r--   8 jsw93 supergroup     188459 2021-08-09 14:58 /data/ghcnd/daily/1866.csv.gz
# -rw-r--r--   8 jsw93 supergroup     237921 2021-08-09 15:03 /data/ghcnd/daily/1867.csv.gz
# -rw-r--r--   8 jsw93 supergroup     257506 2021-08-09 15:01 /data/ghcnd/daily/1868.csv.gz
# -rw-r--r--   8 jsw93 supergroup     305230 2021-08-09 15:03 /data/ghcnd/daily/1869.csv.gz
# -rw-r--r--   8 jsw93 supergroup     355084 2021-08-09 14:58 /data/ghcnd/daily/1870.csv.gz
# -rw-r--r--   8 jsw93 supergroup     478357 2021-08-09 15:02 /data/ghcnd/daily/1871.csv.gz
# -rw-r--r--   8 jsw93 supergroup     652507 2021-08-09 14:59 /data/ghcnd/daily/1872.csv.gz
# -rw-r--r--   8 jsw93 supergroup     734687 2021-08-09 14:56 /data/ghcnd/daily/1873.csv.gz
# -rw-r--r--   8 jsw93 supergroup     820791 2021-08-09 15:03 /data/ghcnd/daily/1874.csv.gz
# -rw-r--r--   8 jsw93 supergroup     904919 2021-08-09 15:05 /data/ghcnd/daily/1875.csv.gz
# -rw-r--r--   8 jsw93 supergroup     999885 2021-08-09 15:06 /data/ghcnd/daily/1876.csv.gz
# -rw-r--r--   8 jsw93 supergroup    1114978 2021-08-09 15:05 /data/ghcnd/daily/1877.csv.gz
# -rw-r--r--   8 jsw93 supergroup    1342400 2021-08-09 15:01 /data/ghcnd/daily/1878.csv.gz
# -rw-r--r--   8 jsw93 supergroup    1538116 2021-08-09 15:07 /data/ghcnd/daily/1879.csv.gz
# -rw-r--r--   8 jsw93 supergroup    1902644 2021-08-09 15:05 /data/ghcnd/daily/1880.csv.gz
# -rw-r--r--   8 jsw93 supergroup    2423109 2021-08-09 15:02 /data/ghcnd/daily/1881.csv.gz
# -rw-r--r--   8 jsw93 supergroup    2781762 2021-08-09 14:57 /data/ghcnd/daily/1882.csv.gz
# -rw-r--r--   8 jsw93 supergroup    3114237 2021-08-09 15:01 /data/ghcnd/daily/1883.csv.gz
# -rw-r--r--   8 jsw93 supergroup    3779904 2021-08-09 15:04 /data/ghcnd/daily/1884.csv.gz
# -rw-r--r--   8 jsw93 supergroup    4351537 2021-08-09 15:05 /data/ghcnd/daily/1885.csv.gz
# -rw-r--r--   8 jsw93 supergroup    4795062 2021-08-09 15:05 /data/ghcnd/daily/1886.csv.gz
# -rw-r--r--   8 jsw93 supergroup    5395545 2021-08-09 15:02 /data/ghcnd/daily/1887.csv.gz
# -rw-r--r--   8 jsw93 supergroup    5818205 2021-08-09 14:59 /data/ghcnd/daily/1888.csv.gz
# -rw-r--r--   8 jsw93 supergroup    6326218 2021-08-09 15:02 /data/ghcnd/daily/1889.csv.gz
# -rw-r--r--   8 jsw93 supergroup    6920839 2021-08-09 14:58 /data/ghcnd/daily/1890.csv.gz
# -rw-r--r--   8 jsw93 supergroup    7253013 2021-08-09 15:01 /data/ghcnd/daily/1891.csv.gz
# -rw-r--r--   8 jsw93 supergroup    8462442 2021-08-09 15:08 /data/ghcnd/daily/1892.csv.gz
# -rw-r--r--   8 jsw93 supergroup   15516245 2021-08-09 14:57 /data/ghcnd/daily/1893.csv.gz
# -rw-r--r--   8 jsw93 supergroup   16567851 2021-08-09 15:01 /data/ghcnd/daily/1894.csv.gz
# -rw-r--r--   8 jsw93 supergroup   17917698 2021-08-09 15:07 /data/ghcnd/daily/1895.csv.gz
# -rw-r--r--   8 jsw93 supergroup   19332812 2021-08-09 15:03 /data/ghcnd/daily/1896.csv.gz
# -rw-r--r--   8 jsw93 supergroup   20872937 2021-08-09 15:08 /data/ghcnd/daily/1897.csv.gz
# -rw-r--r--   8 jsw93 supergroup   21794407 2021-08-09 15:00 /data/ghcnd/daily/1898.csv.gz
# -rw-r--r--   8 jsw93 supergroup   22700952 2021-08-09 15:01 /data/ghcnd/daily/1899.csv.gz
# -rw-r--r--   8 jsw93 supergroup   24336841 2021-08-09 15:00 /data/ghcnd/daily/1900.csv.gz
# -rw-r--r--   8 jsw93 supergroup   31706012 2021-08-09 15:05 /data/ghcnd/daily/1901.csv.gz
# -rw-r--r--   8 jsw93 supergroup   32886384 2021-08-09 15:01 /data/ghcnd/daily/1902.csv.gz
# -rw-r--r--   8 jsw93 supergroup   33660062 2021-08-09 15:02 /data/ghcnd/daily/1903.csv.gz
# -rw-r--r--   8 jsw93 supergroup   34812175 2021-08-09 15:08 /data/ghcnd/daily/1904.csv.gz
# -rw-r--r--   8 jsw93 supergroup   36585272 2021-08-09 15:02 /data/ghcnd/daily/1905.csv.gz
# -rw-r--r--   8 jsw93 supergroup   37481262 2021-08-09 15:07 /data/ghcnd/daily/1906.csv.gz
# -rw-r--r--   8 jsw93 supergroup   38648318 2021-08-09 14:59 /data/ghcnd/daily/1907.csv.gz
# -rw-r--r--   8 jsw93 supergroup   39546046 2021-08-09 15:02 /data/ghcnd/daily/1908.csv.gz
# -rw-r--r--   8 jsw93 supergroup   41606853 2021-08-09 14:58 /data/ghcnd/daily/1909.csv.gz
# -rw-r--r--   8 jsw93 supergroup   43061758 2021-08-09 14:57 /data/ghcnd/daily/1910.csv.gz
# -rw-r--r--   8 jsw93 supergroup   44771484 2021-08-09 15:06 /data/ghcnd/daily/1911.csv.gz
# -rw-r--r--   8 jsw93 supergroup   46604488 2021-08-09 14:56 /data/ghcnd/daily/1912.csv.gz
# -rw-r--r--   8 jsw93 supergroup   47957296 2021-08-09 15:01 /data/ghcnd/daily/1913.csv.gz
# -rw-r--r--   8 jsw93 supergroup   49524502 2021-08-09 15:02 /data/ghcnd/daily/1914.csv.gz
# -rw-r--r--   8 jsw93 supergroup   51064165 2021-08-09 14:56 /data/ghcnd/daily/1915.csv.gz
# -rw-r--r--   8 jsw93 supergroup   52775726 2021-08-09 14:58 /data/ghcnd/daily/1916.csv.gz
# -rw-r--r--   8 jsw93 supergroup   53049849 2021-08-09 15:07 /data/ghcnd/daily/1917.csv.gz
# -rw-r--r--   8 jsw93 supergroup   51751526 2021-08-09 14:57 /data/ghcnd/daily/1918.csv.gz
# -rw-r--r--   8 jsw93 supergroup   51172662 2021-08-09 15:03 /data/ghcnd/daily/1919.csv.gz
# -rw-r--r--   8 jsw93 supergroup   51523715 2021-08-09 15:01 /data/ghcnd/daily/1920.csv.gz
# -rw-r--r--   8 jsw93 supergroup   51976899 2021-08-09 15:01 /data/ghcnd/daily/1921.csv.gz
# -rw-r--r--   8 jsw93 supergroup   52991786 2021-08-09 15:03 /data/ghcnd/daily/1922.csv.gz
# -rw-r--r--   8 jsw93 supergroup   54049204 2021-08-09 15:03 /data/ghcnd/daily/1923.csv.gz
# -rw-r--r--   8 jsw93 supergroup   55120639 2021-08-09 15:03 /data/ghcnd/daily/1924.csv.gz
# -rw-r--r--   8 jsw93 supergroup   55488083 2021-08-09 14:59 /data/ghcnd/daily/1925.csv.gz
# -rw-r--r--   8 jsw93 supergroup   56968872 2021-08-09 15:03 /data/ghcnd/daily/1926.csv.gz
# -rw-r--r--   8 jsw93 supergroup   58011048 2021-08-09 14:59 /data/ghcnd/daily/1927.csv.gz
# -rw-r--r--   8 jsw93 supergroup   58778318 2021-08-09 15:08 /data/ghcnd/daily/1928.csv.gz
# -rw-r--r--   8 jsw93 supergroup   60037200 2021-08-09 15:06 /data/ghcnd/daily/1929.csv.gz
# -rw-r--r--   8 jsw93 supergroup   61790362 2021-08-09 15:05 /data/ghcnd/daily/1930.csv.gz
# -rw-r--r--   8 jsw93 supergroup   63880256 2021-08-09 14:56 /data/ghcnd/daily/1931.csv.gz
# -rw-r--r--   8 jsw93 supergroup   65148533 2021-08-09 15:07 /data/ghcnd/daily/1932.csv.gz
# -rw-r--r--   8 jsw93 supergroup   65800174 2021-08-09 15:05 /data/ghcnd/daily/1933.csv.gz
# -rw-r--r--   8 jsw93 supergroup   66307957 2021-08-09 15:05 /data/ghcnd/daily/1934.csv.gz
# -rw-r--r--   8 jsw93 supergroup   67484740 2021-08-09 15:02 /data/ghcnd/daily/1935.csv.gz
# -rw-r--r--   8 jsw93 supergroup   72120629 2021-08-09 15:07 /data/ghcnd/daily/1936.csv.gz
# -rw-r--r--   8 jsw93 supergroup   73944657 2021-08-09 15:03 /data/ghcnd/daily/1937.csv.gz
# -rw-r--r--   8 jsw93 supergroup   75508065 2021-08-09 15:06 /data/ghcnd/daily/1938.csv.gz
# -rw-r--r--   8 jsw93 supergroup   77887572 2021-08-09 15:01 /data/ghcnd/daily/1939.csv.gz
# -rw-r--r--   8 jsw93 supergroup   80794561 2021-08-09 15:00 /data/ghcnd/daily/1940.csv.gz
# -rw-r--r--   8 jsw93 supergroup   83426962 2021-08-09 14:59 /data/ghcnd/daily/1941.csv.gz
# -rw-r--r--   8 jsw93 supergroup   86080462 2021-08-09 15:02 /data/ghcnd/daily/1942.csv.gz
# -rw-r--r--   8 jsw93 supergroup   87393323 2021-08-09 14:59 /data/ghcnd/daily/1943.csv.gz
# -rw-r--r--   8 jsw93 supergroup   89592105 2021-08-09 15:02 /data/ghcnd/daily/1944.csv.gz
# -rw-r--r--   8 jsw93 supergroup   92888819 2021-08-09 15:01 /data/ghcnd/daily/1945.csv.gz
# -rw-r--r--   8 jsw93 supergroup   93637196 2021-08-09 15:02 /data/ghcnd/daily/1946.csv.gz
# -rw-r--r--   8 jsw93 supergroup   96205468 2021-08-09 14:56 /data/ghcnd/daily/1947.csv.gz
# -rw-r--r--   8 jsw93 supergroup  113945616 2021-08-09 15:00 /data/ghcnd/daily/1948.csv.gz
# -rw-r--r--   8 jsw93 supergroup  129580700 2021-08-09 15:05 /data/ghcnd/daily/1949.csv.gz
# -rw-r--r--   8 jsw93 supergroup  133412298 2021-08-09 14:59 /data/ghcnd/daily/1950.csv.gz
# -rw-r--r--   8 jsw93 supergroup  138488738 2021-08-09 15:06 /data/ghcnd/daily/1951.csv.gz
# -rw-r--r--   8 jsw93 supergroup  140470177 2021-08-09 14:59 /data/ghcnd/daily/1952.csv.gz
# -rw-r--r--   8 jsw93 supergroup  142350529 2021-08-09 15:03 /data/ghcnd/daily/1953.csv.gz
# -rw-r--r--   8 jsw93 supergroup  145311928 2021-08-09 15:06 /data/ghcnd/daily/1954.csv.gz
# -rw-r--r--   8 jsw93 supergroup  148575733 2021-08-09 15:05 /data/ghcnd/daily/1955.csv.gz
# -rw-r--r--   8 jsw93 supergroup  151453029 2021-08-09 15:03 /data/ghcnd/daily/1956.csv.gz
# -rw-r--r--   8 jsw93 supergroup  154780951 2021-08-09 15:01 /data/ghcnd/daily/1957.csv.gz
# -rw-r--r--   8 jsw93 supergroup  156547861 2021-08-09 15:00 /data/ghcnd/daily/1958.csv.gz
# -rw-r--r--   8 jsw93 supergroup  160538756 2021-08-09 15:05 /data/ghcnd/daily/1959.csv.gz
# -rw-r--r--   8 jsw93 supergroup  163916212 2021-08-09 15:07 /data/ghcnd/daily/1960.csv.gz
# -rw-r--r--   8 jsw93 supergroup  169644718 2021-08-09 15:05 /data/ghcnd/daily/1961.csv.gz
# -rw-r--r--   8 jsw93 supergroup  173399072 2021-08-09 15:01 /data/ghcnd/daily/1962.csv.gz
# -rw-r--r--   8 jsw93 supergroup  177698967 2021-08-09 15:00 /data/ghcnd/daily/1963.csv.gz
# -rw-r--r--   8 jsw93 supergroup  178917920 2021-08-09 14:57 /data/ghcnd/daily/1964.csv.gz
# -rw-r--r--   8 jsw93 supergroup  184305621 2021-08-09 15:08 /data/ghcnd/daily/1965.csv.gz
# -rw-r--r--   8 jsw93 supergroup  187038607 2021-08-09 15:05 /data/ghcnd/daily/1966.csv.gz
# -rw-r--r--   8 jsw93 supergroup  189218306 2021-08-09 15:03 /data/ghcnd/daily/1967.csv.gz
# -rw-r--r--   8 jsw93 supergroup  189123086 2021-08-09 15:01 /data/ghcnd/daily/1968.csv.gz
# -rw-r--r--   8 jsw93 supergroup  191403853 2021-08-09 15:02 /data/ghcnd/daily/1969.csv.gz
# -rw-r--r--   8 jsw93 supergroup  192907037 2021-08-09 15:00 /data/ghcnd/daily/1970.csv.gz
# -rw-r--r--   8 jsw93 supergroup  183791008 2021-08-09 14:59 /data/ghcnd/daily/1971.csv.gz
# -rw-r--r--   8 jsw93 supergroup  182786757 2021-08-09 14:59 /data/ghcnd/daily/1972.csv.gz
# -rw-r--r--   8 jsw93 supergroup  191754717 2021-08-09 15:04 /data/ghcnd/daily/1973.csv.gz
# -rw-r--r--   8 jsw93 supergroup  193334332 2021-08-09 14:57 /data/ghcnd/daily/1974.csv.gz
# -rw-r--r--   8 jsw93 supergroup  192712958 2021-08-09 15:07 /data/ghcnd/daily/1975.csv.gz
# -rw-r--r--   8 jsw93 supergroup  192619974 2021-08-09 15:00 /data/ghcnd/daily/1976.csv.gz
# -rw-r--r--   8 jsw93 supergroup  192307060 2021-08-09 15:03 /data/ghcnd/daily/1977.csv.gz
# -rw-r--r--   8 jsw93 supergroup  192906924 2021-08-09 15:08 /data/ghcnd/daily/1978.csv.gz
# -rw-r--r--   8 jsw93 supergroup  193570416 2021-08-09 15:04 /data/ghcnd/daily/1979.csv.gz
# -rw-r--r--   8 jsw93 supergroup  194424553 2021-08-09 14:57 /data/ghcnd/daily/1980.csv.gz
# -rw-r--r--   8 jsw93 supergroup  198552484 2021-08-09 14:57 /data/ghcnd/daily/1981.csv.gz
# -rw-r--r--   8 jsw93 supergroup  200430771 2021-08-09 14:57 /data/ghcnd/daily/1982.csv.gz
# -rw-r--r--   8 jsw93 supergroup  202192765 2021-08-09 15:08 /data/ghcnd/daily/1983.csv.gz
# -rw-r--r--   8 jsw93 supergroup  198974417 2021-08-09 15:02 /data/ghcnd/daily/1984.csv.gz
# -rw-r--r--   8 jsw93 supergroup  197218960 2021-08-09 15:07 /data/ghcnd/daily/1985.csv.gz
# -rw-r--r--   8 jsw93 supergroup  195764826 2021-08-09 14:57 /data/ghcnd/daily/1986.csv.gz
# -rw-r--r--   8 jsw93 supergroup  195735803 2021-08-09 14:57 /data/ghcnd/daily/1987.csv.gz
# -rw-r--r--   8 jsw93 supergroup  196675478 2021-08-09 14:56 /data/ghcnd/daily/1988.csv.gz
# -rw-r--r--   8 jsw93 supergroup  197087415 2021-08-09 14:57 /data/ghcnd/daily/1989.csv.gz
# -rw-r--r--   8 jsw93 supergroup  197232897 2021-08-09 15:00 /data/ghcnd/daily/1990.csv.gz
# -rw-r--r--   8 jsw93 supergroup  197776512 2021-08-09 15:00 /data/ghcnd/daily/1991.csv.gz
# -rw-r--r--   8 jsw93 supergroup  197773341 2021-08-09 15:03 /data/ghcnd/daily/1992.csv.gz
# -rw-r--r--   8 jsw93 supergroup  196173163 2021-08-09 15:05 /data/ghcnd/daily/1993.csv.gz
# -rw-r--r--   8 jsw93 supergroup  195033397 2021-08-09 15:08 /data/ghcnd/daily/1994.csv.gz
# -rw-r--r--   8 jsw93 supergroup  194495954 2021-08-09 15:01 /data/ghcnd/daily/1995.csv.gz
# -rw-r--r--   8 jsw93 supergroup  194661682 2021-08-09 15:07 /data/ghcnd/daily/1996.csv.gz
# -rw-r--r--   8 jsw93 supergroup  192815021 2021-08-09 15:00 /data/ghcnd/daily/1997.csv.gz
# -rw-r--r--   8 jsw93 supergroup  194281477 2021-08-09 14:56 /data/ghcnd/daily/1998.csv.gz
# -rw-r--r--   8 jsw93 supergroup  197167444 2021-08-09 15:01 /data/ghcnd/daily/1999.csv.gz
# -rw-r--r--   8 jsw93 supergroup  198895721 2021-08-09 15:08 /data/ghcnd/daily/2000.csv.gz
# -rw-r--r--   8 jsw93 supergroup  201157448 2021-08-09 15:07 /data/ghcnd/daily/2001.csv.gz
# -rw-r--r--   8 jsw93 supergroup  203270754 2021-08-09 15:01 /data/ghcnd/daily/2002.csv.gz
# -rw-r--r--   8 jsw93 supergroup  207993792 2021-08-09 14:58 /data/ghcnd/daily/2003.csv.gz
# -rw-r--r--   8 jsw93 supergroup  211630534 2021-08-09 14:58 /data/ghcnd/daily/2004.csv.gz
# -rw-r--r--   8 jsw93 supergroup  208866094 2021-08-09 15:06 /data/ghcnd/daily/2005.csv.gz
# -rw-r--r--   8 jsw93 supergroup  210828782 2021-08-09 15:02 /data/ghcnd/daily/2006.csv.gz
# -rw-r--r--   8 jsw93 supergroup  215166023 2021-08-09 15:06 /data/ghcnd/daily/2007.csv.gz
# -rw-r--r--   8 jsw93 supergroup  225927991 2021-08-09 15:05 /data/ghcnd/daily/2008.csv.gz
# -rw-r--r--   8 jsw93 supergroup  230341316 2021-08-09 15:04 /data/ghcnd/daily/2009.csv.gz
# -rw-r--r--   8 jsw93 supergroup  232080599 2021-08-09 15:02 /data/ghcnd/daily/2010.csv.gz
# -rw-r--r--   8 jsw93 supergroup  220857715 2021-08-09 14:58 /data/ghcnd/daily/2011.csv.gz
# -rw-r--r--   8 jsw93 supergroup  216524992 2021-08-09 14:58 /data/ghcnd/daily/2012.csv.gz
# -rw-r--r--   8 jsw93 supergroup  210474832 2021-08-09 14:59 /data/ghcnd/daily/2013.csv.gz
# -rw-r--r--   8 jsw93 supergroup  205377559 2021-08-09 15:03 /data/ghcnd/daily/2014.csv.gz
# -rw-r--r--   8 jsw93 supergroup  207618101 2021-08-09 15:00 /data/ghcnd/daily/2015.csv.gz
# -rw-r--r--   8 jsw93 supergroup  209584081 2021-08-09 15:04 /data/ghcnd/daily/2016.csv.gz
# -rw-r--r--   8 jsw93 supergroup  207342349 2021-08-09 15:06 /data/ghcnd/daily/2017.csv.gz
# -rw-r--r--   8 jsw93 supergroup  151565589 2021-08-17 15:47 /data/ghcnd/daily/2018.csv.gz
# -rw-r--r--   8 jsw93 supergroup  150058584 2021-08-17 15:47 /data/ghcnd/daily/2019.csv.gz
# -rw-r--r--   8 jsw93 supergroup  149689670 2022-03-16 10:45 /data/ghcnd/daily/2020.csv.gz
# -rw-r--r--   8 jsw93 supergroup  146936025 2022-03-16 10:45 /data/ghcnd/daily/2021.csv.gz
# -rw-r--r--   8 jsw93 supergroup   25985757 2022-03-16 10:45 /data/ghcnd/daily/2022.csv.gz

#What is the total size of all the data? How much of that is daily?
[----nz@mathmadslinux2p ~]$ hdfs dfs -du -h /data/ghcnd/
# 15.7 G  125.3 G  /data/ghcnd/daily
# 3.6 K   28.6 K   /data/ghcnd/ghcnd-countries.txt
# 30.9 M  247.4 M  /data/ghcnd/ghcnd-inventory.txt
# 1.1 K   8.5 K    /data/ghcnd/ghcnd-states.txt
# 9.7 M   77.7 M   /data/ghcnd/ghcnd-stations.txt
# 26.0 K  207.9 K  /data/ghcnd/readme.txt

[----nz@mathmadslinux2p ~]$ hdfs dfs -ls /user/bpa78/outputs/ghcnd
# Found 5 items
# drwxr-xr-x   - bpa78 bpa78          0 2022-04-21 11:17 /user/bpa78/outputs/ghcnd/Stations
# drwxr-xr-x   - bpa78 bpa78          0 2022-04-20 23:44 /user/bpa78/outputs/ghcnd/Stations.csv.gz
# drwxr-xr-x   - bpa78 bpa78          0 2022-04-21 15:53 /user/bpa78/outputs/ghcnd/countries
# drwxr-xr-x   - bpa78 bpa78          0 2022-04-18 14:27 /user/bpa78/outputs/ghcnd/distance_NZstations
# drwxr-xr-x   - bpa78 bpa78          0 2022-04-21 17:41 /user/bpa78/outputs/ghcnd/states


###To be check the file size for stations.csv and station.csv.gz
[----nz@mathmadslinux2p ~]$ hdfs dfs -du -h /user/bpa78/outputs/ghcnd/Station.csv.gz
# 0        0      /user/bpa78/outputs/ghcnd/Station.csv.gz/_SUCCESS
# 728.4 K  2.8 M  /user/bpa78/outputs/ghcnd/Station.csv.gz/part-00000-443d5e2e-c5bc-4156-b64c-cdd8238e9a76-c000.csv.gz
# 736.4 K  2.9 M  /user/bpa78/outputs/ghcnd/Station.csv.gz/part-00001-443d5e2e-c5bc-4156-b64c-cdd8238e9a76-c000.csv.gz
# 733.8 K  2.9 M  /user/bpa78/outputs/ghcnd/Station.csv.gz/part-00002-443d5e2e-c5bc-4156-b64c-cdd8238e9a76-c000.csv.gz
# 744.1 K  2.9 M  /user/bpa78/outputs/ghcnd/Station.csv.gz/part-00003-443d5e2e-c5bc-4156-b64c-cdd8238e9a76-c000.csv.gz
[----nz@mathmadslinux2p ~]$ hdfs dfs -du -h /user/bpa78/outputs/ghcnd/Stations
# 0      0       /user/bpa78/outputs/ghcnd/Stations/_SUCCESS
# 2.8 M  11.2 M  /user/bpa78/outputs/ghcnd/Stations/part-00000-f2b9377c-56eb-48e4-9c6a-0253b32fae21-c000.csv
# 2.8 M  11.3 M  /user/bpa78/outputs/ghcnd/Stations/part-00001-f2b9377c-56eb-48e4-9c6a-0253b32fae21-c000.csv
# 2.8 M  11.2 M  /user/bpa78/outputs/ghcnd/Stations/part-00002-f2b9377c-56eb-48e4-9c6a-0253b32fae21-c000.csv
# 2.8 M  11.4 M  /user/bpa78/outputs/ghcnd/Stations/part-00003-f2b9377c-56eb-48e4-9c6a-0253b32fae21-c000.csv

##########Analysis#########

#To start a pyspark shell with 4 executors, 2 cores per executor, 
#4GB of executor memory, 4GB of master memory
start_pyspark_shell -e4 -c2 -w4 -m4

#To get the defult blocksize of HDFS
hdfs getconf -confKey "dfs.blocksize"
#134217728

#How many blocks are required for daily climate summaries for the year 2022?
hdfs dfs -du /data/ghcnd/daily/2022.csv.gz
#25985757  207886056  /data/ghcnd/daily/2022.csv.gz 
#As the file size is less than the default block size of 134217728 bytes which is 128MB  we just need 1 block for daily climate summaries for year 2022 

#How many blocks are required for daily climate summaries for the year 2021?
hdfs dfs -du /data/ghcnd/daily/2021.csv.gz
#146936025  1175488200  /data/ghcnd/daily/2021.csv.gz
#Since the file size is more than the default block size of 134MB we need 2 blocks for daily climate summaries for year 2021. 1 block of 12718297 bytes 128MB and the remaining 12MB in the 2nd block.

#What are the individual block sizes for the year 2022?
hdfs fsck /data/ghcnd/daily/2022.csv.gz -files -blocks
#/data/ghcnd/daily/2022.csv.gz 25985757 bytes, replicated: replication=8, 1 block(s):  OK
#0. BP-700027894-132.181.129.68-1626517177804:blk_1073769759_28939 len=25985757 Live_repl=8

#What are the individual block sizes for the year 2021?
hdfs fsck /data/ghcnd/daily/2021.csv.gz -files -blocks
#/data/ghcnd/daily/2021.csv.gz 146936025 bytes, replicated: replication=8, 2 block(s):  OK
#0. BP-700027894-132.181.129.68-1626517177804:blk_1073769757_28937 len=134217728 Live_repl=8
#1. BP-700027894-132.181.129.68-1626517177804:blk_1073769758_28938 len=12718297 Live_repl=8


#To copy the TMIN_TMAX_NZ.csv from HDFS to local home directory
hdfs dfs -copyToLocal hdfs:///user/bpa78/outputs/ghcnd/TMIN_TMAX_NZ/ /users/home/bpa78

#To count number of rows 
wc -l `find /users/home/bpa78/TMIN_TMAX_NZ/*.* -type f`
#472354 total 
#as we have 83 files the header of each file is added to a row 


#total number of files and folders in outputs/ghcnd folder
[----nz@mathmadslinux2p ~]$ hdfs dfs -ls /user/bpa78/outputs/ghcnd
# Found 7 items
# drwxr-xr-x   - bpa78 bpa78          0 2022-04-21 11:17 /user/bpa78/outputs/ghcnd/Stations
# drwxr-xr-x   - bpa78 bpa78          0 2022-04-20 23:44 /user/bpa78/outputs/ghcnd/Stations.csv.gz
# drwxr-xr-x   - bpa78 bpa78          0 2022-04-23 21:56 /user/bpa78/outputs/ghcnd/TMIN_TMAX_NZ
# drwxr-xr-x   - bpa78 bpa78          0 2022-04-23 22:39 /user/bpa78/outputs/ghcnd/avgRainfallCountry
# drwxr-xr-x   - bpa78 bpa78          0 2022-04-21 15:53 /user/bpa78/outputs/ghcnd/countries
# drwxr-xr-x   - bpa78 bpa78          0 2022-04-23 19:14 /user/bpa78/outputs/ghcnd/distance_NZstations
# drwxr-xr-x   - bpa78 bpa78          0 2022-04-21 17:41 /user/bpa78/outputs/ghcnd/states
