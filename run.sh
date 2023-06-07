#python3 database.py save #Use this to Save Database Entities to Files
rm -r ./all_programs
# unzip ./all_programs.zip
python3 edit_files.py
python3 database.py

COINS=("KAVAUSDT" "RENUSDT" "COMPUSDT" "DYDXUSDT" "SOLUSDT")

for symbol in "${COINS[@]}"
do
    cd /home/mrAlimaBot/all_programs/5m/"$symbol"/
    pm2 start ./core_5m_"$symbol".py --interpreter python3
    sleep 15
done