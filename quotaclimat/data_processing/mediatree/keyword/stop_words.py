# Based on advertising
STOP_WORDS = [
    "bonus écologique"
    ,"groupe verlaine isolation thermique"
    ,"haute isolation thermique fabriqué en france"
    ,"ou covoiturage tous les moyens sont bon"
    ,"blablacar et recevez cent euros de prime covoiturage"
    ,"conditions sébastien point fr covoiturage"
    ,"sur dacia point fr covoiturage"
    ,"dacia point fr un covoiturage"
    ,"engie peut vous aider par exemple avec des panneaux solaires"
    ,"verlaine photovoltaïques"
    ,"verlaine le climat"
    ,"photovoltaïque groupe"
    ,"groupe verlaine isolation photovoltaïques"
    ,"photovoltaïques vie"
    ,"garanti photovoltaïques"
    ,"photovoltaïque verlaine"
    ,"installe des panneaux photovoltaïques et des bornes de recharge"
    ,"le leader du photovoltaïque chez les particulier"
    ,"panneaux solaires pour produire votre énergie"
    ,"installateur panneaux de photovoltaïques"
    ,"installateurs de panneaux photovoltaïques"
    ,"installateur de panneaux photovoltaïques"
    ,"installateur de panneaux solaires"
    ,"installateurs de panneaux solaires"
    ,"pompes entretien à pour chaleur pompes et à photovoltaïque"
    ,"panneaux solaires groupe"
    ,"pour réduire votre facture d' électricité en installant des panneaux solaires"
    ,"panneaux solaires j' agis avec engie"
    ,"panneaux solaires j' agis avec kendji"
    ,"les tuiles se transforme en panneaux solaires"
    ,"en installant des panneaux solaires j' agis" 
    ,"panneaux solaires face à l' inflation" 
    ,"l'isolation le chauffage les panneaux solaires" 
    ,"installer vos panneaux solaires produire" 
    ,"la crème solaire pour les panneaux solaires" 
    ,"panneaux solaires c' est moi qui" 
    ,"panneaux solaires installation matériel démarches" 
    ,"panneaux solaires installation matérielle démarches"
    ,"la solution de panneaux solaires qui vous aide" 
    ,"installant des panneaux solaires usagés avec" 
    ,"installant des panneaux solaires usagers avec" 
    ,"vous équiper de panneaux solaires et faire des" 
    ,"panneaux solaires découvrez les offres"  
    ,"on monte les panneaux solaires que j'ai"
    ,"énergie solaire avec avicenne"
    ,"panneaux photovoltaïques garanti à vie"
    ,"on installe des panneaux photovoltaïques borne de recharge"
    ,"on installe des panneaux photovoltaïques des bornes de recharge"
    ,"le leader du photovoltaïque"
    ,"en train d"
    ,"consigne de vote"
    ,"climat de confiance"
    ,"huile de coude est aussi une énergie renouvelable"
    ,"huile de coude était aussi une énergie renouvelable"
    ,'huile de coude étaient aussi une énergie renouvelable'
    ,'huile de coude étaient aussi est aussi une énergie renouvelable '
    ,"franchisés dans les énergies renouvelable"
    ,"franchisé dans les énergies renouvelable"
    ,'énergie renouvelable stéphane'
    ,"énergie renouvelable pour moins polluer"
    ,"énergie renouvelable pour dépenser moins"
    ,"par des garanties d' origine renouvelable"
    ,"énergies renouvelables groupe verlaine"
    ,"borne de recharge offert"
    ,"edf installe votre borne de recharge"
    ,"pour faire installer ma borne de recharge"
    ,"si vous avez une borne de recharge partager"
    ,"une borne de recharge bien installé"
    ,"vous avez une borne de recharge" 
    ,"en ce moment borne de recharge"
    ,"profiter de la borne de recharge offert"
    ,"borne de recharge offerte" 
    ,"faire installer une borne de recharge" 
    ,"en copropriété une borne de recharge" 
    ,"peugeot vous offre la borne de recharge" 
    ,"pour les plus branchées la borne de recharge" 
    ,"leasing électrique à"
    ,"gouvernemental mon leasing électrique"
    ,"éligibles au leasing électrique"
    ,"éligible au leasing électrique"
    ,"offre mon leasing électrique"
    ,"leasing électrique économise"
    ,"climatique c' est pour ça que je suis au crédit coopératif"
    ,"installateur de pompe à chaleur air"
    ,"pompe à chaleur atlantique"
    ,"et plus durable et avec spoticar"
    ,"l' éolien jeanjean"
    ,"bd thierry rénovation énergétique point fr"
    ,"le mouvement de la rénovation énergétique"
    ,"mon parcours rénovation diagnostic"
    ,"leroy merlin la rénovation énergétique"
    ,"rénovation énergétique la météo avec groupe verlaine"
    ,"rénovation globale groupe verlaine"
    ,"en machine le gaspillage"
    ,"l' économie circulaire liddell"
    ,"l' économie circulaire carrefour"
    ,"pour l' économie circulaire bonjour"
    ,"sans ogm et cent pour cent"
    ,"nourries sans ogm"
    ,"nourries sans ogm et ogm cent"
    ,"Salon de jardin miami fabriqué avec quatre-vingts pour cent de matériaux recyclés"
    ,"l' entreprise de julie fabrique des meubles en matériaux recyclés"
    ,"jardin miami fabriqué avec quatre-vingts pour cent de matériaux recyclés"
    ,"trier ses déchets ça fait partie du jeu"
    ,"trier ses déchets même si ce n' est pas toujours évident"
    ,"voire règlement services déchets"
    ,"salon de jardin miami fabriqué avec quatre-vingts pour cent de matériaux recyclés"
    ,"l' entreprise de julie fabrique des meubles en matériaux recyclés"
    ,"jardin miami fabriqué avec quatre-vingts pour cent de matériaux recyclés"
    ,"est à fond sur le tri sélectif"
    ,"transition énergétique baisse de lumière"
    ,"transition énergétique co"
    ,"transition énergétique nos lumières"
    ,"transition énergétique lumière"
    ,"transition énergétique on est aux lumières"
    ,"transition énergétique ces lots lumière"
    ,"transition énergétique c' est aux lumières"
    ,"vendez votre voiture point fr"
    ,"votre voiture obtenait un prix"
    ,"recharge ma brosse à dents"
    ,"qui bossait dans l' eau" # maif
    ,"je croyais que dégât des eaux" # maif
    ,"un goût généreux et le respect de la nature sans résidu de pesticides" # tramier
    ,"écoresponsables offre valable" #aldi
    ,"écoresponsable offre valable" #aldi
    ,"vergers écoresponsables" #aldi
    ,"factures avec l' installation de panneaux solaires"
    ,"panneaux solaires et faire des économies carrefour"
    ,"garantie carrefour énergie"
    ,"économies sur votre énergie"
    ,"énergie c' est trop cher je vous réponds fait comme marc"
    ,"énergie une étude révèle que six français sur dix"
    ,"énergie choisissez edf"
    ,"voiture de l' année"
    ,"voiture année"
    ,"carrefour point fr les publicités de voiture"
    ,"aramisauto nos voitures"
    ,"aramis auto nos voitures"
    ,"voiture sur aramisauto"
    ,"voiture au meilleur prix"
    ,"l' univers de la voiture d' occasion"
    ,"voiture sur aramis auto"
    ,"future voiture d' occasion reconditionnée"
    ,"vous appel parce que ma voiture"
    ,"disposition une voiture le temps"
    ,"bornes de recharge et des mois de loyer"
    ,"cent pour cent électriques bornes de recharge"
    ,"isolation par l' extérieur"
    ,"votre énergie avec l' installation de panneaux solaires"
    ,"offrez à vos clients un devis précis pour améliorer la performance énergétique"
]
    