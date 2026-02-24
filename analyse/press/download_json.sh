#!/bin/bash


folders1=(
    medias_3294162846
    medias_3473138825
    medias_615031662
    medias_1239527030
    medias_21943587
    medias_1493643953
    medias_5709212069
    medias_403821594
    medias_47682120
    medias_18208728
    medias_30161301136
    medias_47682120
)


for d in ${folders1[@]}; do
    mkdir -p "data/json/$d" && scp "ubuntu@51.159.169.250:/home/ubuntu/scrapping-instagram/medias/$d/2026-*.json" "./data/json/$d/." 
    mkdir -p "data/json/$d" && scp "ubuntu@51.159.169.250:/home/ubuntu/scrapping-instagram/medias/$d/2025-*.json" "./data/json/$d/." 
done

folders2=(
    medias_358910360
    medias_2367282612
    medias_1422075705
    medias_270328500
    medias_8709600632
    medias_535966400
    medias_2244063477
    medias_1683607466
    medias_1415438075
    medias_1600123611
    medias_1683617003
    medias_7288700843
    medias_607273572
    medias_74164885293
)


for d in ${folders2[@]}; do
    mkdir -p "data/json/$d" && scp "ubuntu@51.159.169.250:/home/ubuntu/scrapping-instagram2/medias/$d/2026-*.json" "./data/json/$d/."
    mkdir -p "data/json/$d" && scp "ubuntu@51.159.169.250:/home/ubuntu/scrapping-instagram2/medias/$d/2025-*.json" "./data/json/$d/."
done


folders3=(
    medias_1457924076
    medias_723073126
    medias_2294917766
    medias_1537659317
    medias_5782473796
    medias_7189714862
    medias_380942787
    medias_8431122676
    medias_47179417592
    medias_608863410
    medias_5897493356
    medias_1110003767
    medias_65236293692
    medias_37241632
    medias_1481151026
)


for d in ${folders3[@]}; do
    mkdir -p "data/json/$d" && scp "ubuntu@51.159.169.250:/home/ubuntu/scrapping-instagram3/medias/$d/2026-*.json" "./data/json/$d/."
    mkdir -p "data/json/$d" && scp "ubuntu@51.159.169.250:/home/ubuntu/scrapping-instagram3/medias/$d/2025-*.json" "./data/json/$d/."
done


folders4=(
    medias_30436739
    medias_2809475279
    medias_51083799981
    medias_55511109066
    medias_212082608
    medias_5996039291
    medias_20132715
    medias_1492533776
    medias_451987162
    medias_3623974806
    medias_558653212
)


for d in ${folders4[@]}; do
    mkdir -p "data/json/$d" && scp "ubuntu@51.159.169.250:/home/ubuntu/scrapping-instagram4/medias/$d/2026-*.json" "./data/json/$d/."
    mkdir -p "data/json/$d" && scp "ubuntu@51.159.169.250:/home/ubuntu/scrapping-instagram4/medias/$d/2025-*.json" "./data/json/$d/."
done