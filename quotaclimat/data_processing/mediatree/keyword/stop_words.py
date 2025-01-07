# Based on advertising - should be a SQL table "Stop_Word"
STOP_WORDS = [
    "bonus écologique"
    ,"haute isolation thermique fabriqué en france"
    ,"robert laine isolation thermique"
    ,"robert helen isolation thermique"
    ,"robert allen isolation thermique"
    ,"concept spécialiste de l' isolation thermique"
    ,"thermiques groupe verlaine"
    ,"ou covoiturage tous les moyens sont bon"
    ,"blablacar et recevez cent euros de prime covoiturage"
    ,"conditions sébastien point fr covoiturage"
    ,"sur dacia point fr covoiturage"
    ,"dacia point fr un covoiturage"
    ,"engie peut vous aider par exemple avec des panneaux solaires"
    ,"verlaine le climat"
    ,"photovoltaïque groupe"
    ,"contrat d' entretien pour pompes à chaleur et photovoltaïque" # verlaine
    ,"groupe verlaine pro installations photovoltaïque"
    ,"groupe verlaine pro installation photovoltaïque"
    ,"installateur groupe de verlaine panneaux installateur photovoltaïques de garantie panneaux à photovoltaïques"
    ,"panneaux à photovoltaïques garantis"
    ,"boosté vos économies travaux d' isolation bornes de recharge"
    ,"booster vos économies travaux d' isolation bornes de recharge"
    ,"il y a des affaires à faire sur l' isolation le chauffage les panneaux solaires"
    ,"il y a plein d' offre sur l' isolation le chauffage les panneaux solaires"
    ,"fenêtre double vitrage haute isolation thermique fabriqués dans notre usi"
    ,"verlaine isolation centrale photovoltaïque pompes à chaleur"
    ,"verlaine isolation centrale photovoltaïque et pompes à chaleur"
    ,"verlaine isolation centrale photovoltaïque"
    ,"verlaine isolation photovoltaïques pompes à chaleur"
    ,"verlaine isolation photovoltaïque"
    ,"verlaine isolation thermique"
    ,"verlaine isolation"
    ,"verlaine centrale photovoltaïque"
    ,"verlaine photovoltaïques isolation photovoltaïque"
    ,"centrale photovoltaïque de groupe"
    ,"photovoltaïque chaleur groupe verlaine"
    ,"verlaine photovoltaïque"
    ,"photovoltaïque pompes à chaleur pour une rénovation globale groupe verlaine"
    ,"photovoltaïques pompes à chaleur pour une rénovation globale"
    ,"photovoltaïques vie"
    ,"garanti photovoltaïques"
    ,"photovoltaïque verlaine"
    ,"installation de panneaux photovoltaïques vous avez dit"
    ,"installation de panneaux photovoltaïques et bien merci"
    ,"centrales photovoltaïques pour seulement"
    ,"vation énergétique du toit solaire photovoltaïque"
    ,"installation photovoltaïque de trois"
    ,"photovoltaïque de groupe verlaine"
    ,"installation photovoltaïque peut vous faire économiser jusqu'"
    ,"installation photovoltaïque et garantis vingt-cinq ans"
    ,"centrale photovoltaïque et pompes à chaleur groupe verlaine"
    ,"centrale photovoltaïque pompes à chaleur groupe verlaine"
    ,"pompes à chaleur groupe verlaine"
    ,"climat de confiance"
    ,"panneau photovoltaïque et bornes de recharge thompson groupe verlaine"
    ,"photovoltaïque pour réduire vos factures d' électricité groupe verlaine"
    ,"centrale photovoltaïque mobile facile"
    ,"photovoltaïque groupe verlaine"
    ,"photovoltaïque de groupe verlaine"
    ,"verlaine centrale photovoltaïque"
    ,"centrale photovoltaïque a posé en toute simplicité"
    ,"centrale photovoltaïque à poser en toute simplicité"
    ,"photovoltaïque de quatre"
    ,"centrale photovoltaïque produisait et consommer votre propre énergie"
    ,"verlaine isolation photovoltaïques pompes photovoltaïque"
    ,"verlaine isolation photovoltaïques une pompe"
    ,"verlaine isolation centrales photovoltaïques"
    ,"verlaine isolation photovoltaïque et pompe à chaleur"
    ,"verlaine isolation centrales photovoltaïques et pompes"
    ,"verlaine isolation photovoltaïques pompes à chaleur"
    ,"verlaine isolation photovoltaïque pompe à chaleur"
    ,"votre projet de centrale photovoltaïque"
    ,"verlaine installation de centrales photovoltaïques"
    ,"green installe votre centrale photovoltaïque"
    ,"on refait son isolation change sans chauffage"
    ,"installe des panneaux photovoltaïques et des bornes de recharge"
    ,"photovoltaïques et des bornes de recharge pour que ça profite"
    ,"photovoltaïques et des bornes de recharge et pour que ça profite"
    ,"photovoltaïques des bornes de recharge et pour que ça profite"
    ,"photovoltaïques borne de recharge et pour que ça profite"
    ,"bornes de recharge et pour que ça profite"
    ,"bornes de recharge et d' un"
    ,"bornes de recharge et deux mois"
    ,"euros par mois bornes de recharge"
    ,"pourcent de consommation d' énergie avec le leader du photovoltaïque"
    ,"leader du photovoltaïque chez les particulier"
    ,"leader foot du photovoltaïque"
    ,"leader du photovoltaïque chez du photovoltaïque"
    ,"leader du photovoltaïque chez photovoltaïque"
    ,"vers plus de sobriété l' énergie qui nous unit c' est sûr enercoop"
    ,"panneaux solaires pour produire votre énergie"
    ,"installation des panneaux solaires c' est moi"
    ,"à la crème solaire c' est pour les panneaux solaires"
    ,"photovoltaïque centrale de photovoltaïque"
    ,"panneaux de photovoltaïques panneaux avec photovoltaïques contrat avec demain"
    ,"panneaux avec photovoltaïques contrat"
    ,"installateur panneaux de photovoltaïque"
    ,"photovoltaïque chez les particuliers à passer au solaire" #edf
    ,"installateur de de panneaux photovoltaïque"
    ,"installateurs de panneaux photovoltaïque"
    ,"panneaux photovoltaïque photovoltaïques garanti"
    ,"photovoltaïques garantie à"
    ,"installateur de panneaux photovoltaïque"
    ,"panneaux photovoltaïques et même sur les vêtements"
    ,"installateur de panneaux solaires"
    ,"installateurs de panneaux solaires"
    ,"verlaine installation de panneaux solaires"
    ,"votre expert en panneaux solaires" #  mon kits solaires point fr
    ,"le top des panneaux solaires"
    ,"panneaux solaires garantie à vie"
    ,"euros équipé de panneaux solaires avec carrefour énergie"
    ,"euros équipée de panneaux solaires"
    ,"euros équipée de panneaux solaire"
    ,"euros équipé de panneaux solaires"
    ,"euros équipés de panneaux solaires"
    ,"euros équipées de panneaux solaires"
    ,"je finis d' installer les panneaux solaires"
    ,"panneaux solaires avec carrefour énergie"
    ,"d' achat comme avec les panneaux solaires bifaces sial"
    ,"relations menuiserie panneaux solaires ou tout autre projet de rénovation"
    ,"isolation menuiseries panneaux solaires ou tout autre projet de rénovation"
    ,"isolation menuiserie panneaux solaires"
    ,"je finis d' installer mes panneaux solaire"
    ,"panneaux solaires by facial"
    ,"pompe à chaleur ou le panneau solaire groupe verlaine"
    ,"vous-même vos panneaux solaires"
    ,"une suspension à énergie solaire pour seulement quatre-vingt"
    ,"végétation omniprésente panneaux solaires point d' eau et solutions innovante"
    ,"monte les panneaux solaires que j' ai commandé chez oscaro"
    ,"fameux panneaux solaires cette batterie"
    ,"chauffage les panneaux solaires pour faire des économies"
    ,"chauffages les panneaux solaires pour faire des économies"
    ,"les panneaux solaires pour faire des économies d' énergie"
    ,"avec des panneaux solaires c' est jusqu' à mille cinq"
    ,"de panneaux solaires cinq mille huit"
    ,"en panneaux solaires c' est une banque décidé"
    ,"en panneaux solaires est une bande décidé"
    ,"économies d' énergie impact environnemental des vies"
    ,"économies d' énergie l' impact environnemental des vies"
    ,"ou des entreprises font des économies d' énergie ou l' impact environnemental des vies"
    ,"font des économies d' énergie ou l' impact environnemental"
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
    ,"panneaux photovoltaïques garanti"
    ,"photovoltaïques avec garanti"
    ,"on installe des panneaux photovoltaïques borne de recharge"
    ,"on installe des panneaux photovoltaïques des bornes de recharge"
    ,"mille bornes de recharge entre particuliers"
    ,"verlaine installateur de panneaux photovoltaïques photovoltaïque"
    ,"verlaine installateur de panneaux photovoltaïque"
    ,"verlaine installations photovoltaïques photovoltaïque"
    ,"photovoltaïque vie"
    ,"panneaux contrat photovoltaïques"
    ,"leader photovoltaïque du photovoltaïque"
    ,"leader photovoltaïque du chez photovoltaïque"
    ,"panneaux photovoltaïques avec contrat"
    ,"énergie avec le leader du photovoltaïque photovoltaïque"
    ,"photovoltaïque leader chez du les photovoltaïque"
    ,"leader photovoltaïque"
    ,"leader football du photovoltaïque"
    ,"leader chez du les photovoltaïque"
    ,"chez les photovoltaïque particuliers"
    ,"en train d"
    ,"consigne de vote"
    ,"climat de confiance"
    ,"huile de coude est aussi une énergie renouvelable"
    ,"huile de coude était aussi une énergie renouvelable"
    ,'huile de coude étaient aussi une énergie renouvelable'
    ,'huile de coude étaient aussi est aussi une énergie renouvelable '
    ,"franchisés dans les énergies renouvelable"
    ,"franchisé dans les énergies renouvelable"
    ,'énergie renouvelable stéphan'
    ,"énergie renouvelable pour moins polluer"
    ,"énergie renouvelable pour dépenser moins"
    ,"grand acheteur d' énergie renouvelable"
    ,"énergie renouvelable au monde"
    ,"par des garanties d' origine renouvelable"
    ,"verlaine isolation centrales photovoltaïques pompes à chaleur"
    ,"verlaine programme centrale avec photovoltaïque"
    ,"verlaine installateur de pac le photovoltaïque"
    ,"euro de plus une centrale photovoltaïque"
    ,"verlaine isolation centrales photovoltaïques et pompes à chaleur"
    ,"installation de panneaux photovoltaïques plus d' infos sur tugo"
    ,"groupe verlaine panneaux photovoltaïques et bornes de recharge"
    ,"verlaine installation de centrale photovoltaïque"
    ,"verlaine centrales photovoltaïque"
    ,"verlaine l' expert du photovoltaïque"
    ,"verlaine propres installations photovoltaïque"
    ,"doré un poulet votre installation photovoltaïque"
    ,"programme avec solar box solutions photovoltaïques"
    ,"verlaine installation photovoltaïque"
    ,"verlaine installations photovoltaïque"
    ,"expert en installation de panneaux photovoltaïque"
    ,"green installe votre centrales photovoltaïque"
    ,"green solution photovoltaïque"
    ,"lantique alimenté par une centrale photovoltaïque"
    ,"green solutions photovoltaïques"
    ,"energy le kit de huit panneaux photovoltaïque"
    ,"énergie le kit de huit panneaux photovoltaïque"
    ,"installation photovoltaïque garanti"
    ,"verlaine isolation photovoltaïque centrale photovoltaïque et pompes à chaleur"
    ,"installations photovoltaïques garantis"
    ,"photovoltaïques photovoltaïques garanti"
    ,"photovoltaïques vingt-cinq"
    ,"centrale photovoltaïques mobile"
    ,"photovoltaïques à garanti"
    ,"photovoltaïques garantis à vie"
    ,"rlaine installateur de panneaux de photovoltaïques"
    ,"installateur panneaux de photovoltaïques panneaux photovoltaïques"
    ,"énergies renouvelables groupe verlaine"
    ,"spécialisée dans les énergies renouvelables"
    ,"spécialisée dans les énergies renouvelables"
    ,"offrant un écosystème technologique durable"
    ,"borne de recharge offert"
    ,"profitez de la borne de recharge"
    ,"plus de mille bornes de recharge"
    ,"localise une borne de recharge"
    ,"edf installe votre borne de recharge"
    ,"pour faire installer ma borne de recharge"
    ,"borne de recharge partager"
    ,"localisent une borne de recharge"
    ,"bornes de recharge offert"
    ,"presque impérativement avoir une borne de recharge"
    ,"bornes de recharge et des mois de loyer"
    ,"bornes de recharge et deux"
    ,"si vous avez une borne de recharge partager"
    ,"cent pour cent électriques bornes de recharge"
    ,"cent des panneaux photovoltaïques"
    ,"votre borne de recharge que vous habitez"
    ,"borne de recharge j' ai pas hésité"
    ,"edf installe votre bornes de recharge"
    ,"bornes de recharge et installation offerte"
    ,"bornes de recharge et d' un mois"
    ,"une borne de recharge bien installés c' est simple"
    ,"borne de recharge et deux mois"
    ,"une borne de recharge bien installé"
    ,"vous avez une borne de recharge" 
    ,"en ce moment borne de recharge"
    ,"borne de recharge portes ouvertes"
    ,"offre la borne de recharge"
    ,"borne de recharge est offert"
    ,"borne de recharge offert" 
    ,"faire installer une borne de recharge" 
    ,"en copropriété une borne de recharge" 
    ,"pour les plus branchées la borne de recharge" 
    ,"frigos on installe des panneaux photovoltaïques et des bornes de recharge"
    ,"frigo on installe des panneaux photovoltaïques et des bornes de recharge"
    ,"frigos on installe des panneaux on photovoltaïques"
    ,"leasing électrique à"
    ,"dispositif mon leasing électrique"
    ,"gouvernemental mon leasing électrique"
    ,"éligibles au leasing électrique"
    ,"éligible au dispositif mon leasing électrique"
    ,"éligible au leasing électrique"
    ,"zéro euro bonus aide au leasing électrique"
    ,"leasing électrique avec renault"
    ,"ce leasing électrique ça rendait vraiment la voiture"
    ,"offre mon leasing électrique"
    ,"leasing électrique économise"
    ,"par mois seulement grâce à mon leasing électrique"
    ,"climatique c' est pour ça que je suis au crédit coopératif"
    ,"installateur de pompe à chaleur air"
    ,"pompe à chaleur atlantique"
    ,"pompe à chaleur je suis fait pour la chaleur"
    ,"isolation photovoltaïque et pompe à chaleur pour une réno"
    ,"centrale photovoltaïque du futur" # thompson
    ,"centrale du photovoltaïque futur"
    ,"centrale la photovoltaïque scène du futur" # thompson
    ,"centrale photovoltaïque à partir"
    ,"et plus durable et avec spoticar"
    ,"l' éolien jeanjean"
    ,"rénovation énergétique point fr"
    ,"la rénovation énergétique la météo avec"
    ,"papa la rénovation énergétique"
    ,"mouvement de la rénovation énergétique"
    ,"la rénovation énergétique réussie c' est simple"
    ,"rénovation énergétique avec point p"
    ,"projets de rénovation énergétique avec"
    ,"lancé dans la rénovation énergétique de sa maison"
    ,"lance dans la rénovation énergétique de sa maison"
    ,"lancer dans la rénovation énergétique de sa maison"
    ,"rénovation énergétique rendez-vous sur"
    ,"rénovation énergétique contacté effie"
    ,"rénovation énergétique contacter effie"
    ,"rénovation énergétique contacté effie"
    ,"rénovation énergétique contactés effie"
    ,"rénovation énergétique contactés effie"
    ,"rénovation énergétique contacté efi"
    ,"rénovation énergétique groupe verlaine"
    ,"des travaux de rénovation énergétique jusqu' au"
    ,"edf c' est une rénovation énergétique"
    ,"rénovation énergétique c' était la météo avec groupe verlaine"
    ,"pour votre rénovation énergétique france renov le service public"
    ,"rendre la rénovation énergétique accessible au plus grand nombre à découvrir"
    ,"de rénovation énergétique avec castorama"
    ,"rénovation énergétique rendez vous "
    ,"pour la rénovation énergétique par contre"
    ,"experts de la rénovation énergétique il te conseille"
    ,"la rénovation énergétique il te conseille"
    ,"mon parcours rénovation diagnostic"
    ,"merlin la rénovation énergétique"
    ,"rénovation énergétique la météo avec groupe verlaine"
    ,"photovoltaïque et pompe à chaleur pour une rénovation"
    ,"rénovation globale groupe verlaine"
    ,"bien démarrer votre rénovation énergétique"
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
    ,"nous-mêmes du plastique recyclé"
    ,"nous-même du plastique recyclé"
    ,"nous-mêmes le plastique recyclé"
    ,"nos même du plastique recyclé"
    ,"trois usines exclusivement dédiée aux recyclage"
    ,"trois usines exclusivement dédié au recyclage"
    ,"trois usines exclusivement dédiés au recyclage"
    ,"trois usines exclusivement dédiées au recyclage"
    ,"vivement dédiées au recyclage"
    ,"un gouvernement de recyclage"
    ,"en france le recyclage brise" #carglass
    ,"recyclage du pare-brise" #carglass
    ,"pare-brise de sa nouvelle voiture"
    ,"les trajets courts fini la voiture"
    ,"votre voiture huit en sérénité"
    ,"pare-brise pour les recycler"
    ,"pare-brises pour les recycler"
    ,"paris pour les recycler"
    ,"trier devient plus simple"
    ,"grâce à vous qui trier vos bouteilles"
    ,"nous les recycle pour en faire de nouvelles"
    ,"cristalline pour travail le de recyclage"
    ,"cristalline est capable de recycler"
    ,"cristaline est capable de recycler"
    ,"cristallin est capable de recycler"
    ,"cristalline et capables de recycler"
    ,"cristallines est capable de recycler"
    ,"batterie chez norauto pour la recycler"
    ,"cristaline pour le recyclage"
    ,"cristalline pour le recyclage"
    ,"recyclage du le pare-brise"
    ,"les réparer ou les recycler boulanger"
    ,"point fr le geste utile pour recycler"
    ,"recycler les deux sont garantis"
    ,"avec mma chez soi pièce neuve ou recycler"
    ,"cristalline et capable de recycler"
    ,"cristallines et capable de recycler"
    ,"de recycler de recycler" # bug mediatree
    ,"eau cristalline"
    ,"trier vos bouteilles nous les recycler"
    ,"bouteilles nous les recyclant pour en"
    ,"recycler des lunettes"
    ,"est capable de recycler recycler"
    ,"recycler des milliers de tonnes"
    ,"nous les recycler pour en faire"
    ,"recycler en disant on a déjà recycler"
    ,"recycler en dix ans on a déjà recycler"
    ,"recycler en dix ans on a déjà recyclé"
    ,"recycler et créer des contrats"
    ,"pour recycler votre épave"
    ,"la recycler la vieille"
    ,"les notices de panneaux photovoltaïques"
    ,"photovoltaïque pour professionnels" # veraline
    ,'leader du photovoltaïque'
    ,"grâce aux dons à la réparation au recyclage"
    ,"renforcement de nos filières de recyclage"
    ,"est partout en france le recyclage de pare-brise"
    ,"est le royaume du recyclage il faut juste"
    ,"en termes de tri des déchets le recyclage ou d' énergies renouvelables"
    ,"recyclage depuis trente ans" # cristalline
    ,"cristaline l' eau préférée"
    ,"eau minérale naturelle"
    ,"packs d' eau"
    ,"d' gien eau"
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
    ,"recharge ma brosse"
    ,"tu sais l' autonomie la recharge rapide"
    ,"qui bossait dans l' eau" # maif
    ,"je croyais que dégât des eaux" # maif
    ,"de pesticides tramier" # tramier
    ,"ingrédients peut transformer sans pesticides"
    ,"des pommes gala avec moins de pesticides"
    ,"écoresponsables offre valable" #aldi
    ,"écoresponsable offre valable" #aldi
    ,"vergers écoresponsables" #aldi
    ,"responsable solutions énergétiques"
    ,"pratiques pour un quotidien plus responsable afin de"
    ,"factures avec l' installation de panneaux solaires"
    ,"son panneau solaire apporteront"
    ,"panneaux solaires pour faire des économies d' énergie"
    ,"panneaux solaires et faire des économies carrefour"
    ,"ou des panneaux solaires plus respectueux"
    ,"chocolat plus respectueux de l' environnement"
    ,"garantie carrefour énergie"
    ,"économies sur votre énergie"
    ,"énergie c' est trop cher je vous réponds fait comme marc"
    ,"énergie une étude révèle que six français sur dix"
    ,"énergie choisissez edf"
    ,"énergie et notre avenir économisant"
    ,"mission décarbonation avec edf"
    ,"objectif c' est de réduire ses émissions de co deux"
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
    ,"isolation par l' extérieur"
    ,"votre énergie avec l' installation de panneaux solaires"
    ,"offrez à vos clients un devis précis pour améliorer la performance énergétique"
    ,"augmenter la performance énergétique et la"
    ,"performance énergétique de votre logement groupe verlaine"
    ,"groupe verlaine au service du développement durable"
    ,"gaz très haute performance énergétique plus confortable"
    ,"pergola bioclimatique"
    ,"on imagine des murs végétalisé"
    ,"pêche durable liddell"
    ,"pêche durable littell"
    ,"pêche durable l' idéal"
    ,"saumon ici issus de la pêche durable"
    ,"maprimerénov leroy"
    ,"maprimerénov et économiser jusqu' à"
    ,"maprimerénov une offre comme ça même chez lapeyre"
    ,"comment bénéficier de maprimerénov sur ton projet des experts"
    ,"rénovation énergétique rendez vous sur france"
    ,"aux aides maprimerénov et de faisabilité sous condition"
    ,"leroy merlin quand on refait son isolation"
    ,"engie home services et installer une pompe à chaleur"
    ,"parc saint-paul"
    ,"parc saint paul"
    ,"parc strasbourgeois"
    ,"parc de la villette"
    ,"parc machines"
    ,"parc bordelais"
    ,"parc astérix"
    ,"parc automobile"
    ,"parc de la tête d' or"
    ,"parc lumineux"
    ,"parc d' attraction"
    ,"parc immobilier"
    ,"parc animalier"
    ,"atmosphère cinéma"
    ,"économies d' énergie l' impact environnemental des vies"
    ,"économies d' énergie impact environnemental des vies"
    ,"compliqué d' économiser de l' énergie alors engie vous aide"
    ,"pour faire des économies d' énergie juliette"
    ,"elle voudrait faire des économies de chauffage"
    ,"bonus pompe à chaleur d' edf"
    ,"installer un système plus écologiques comme une pompe à chaleur air eau"
    ,"budget chauffage votre pompe à chaleur"
    ,"budget chauffage la pompe à chaleur"
    ,"tu vois loulou on a bien fait de refaire l' isolation" # france tirer les neuf pts gauff point fr
    ,"système plus écologique comme une pompe à chaleur" # france tirer les neuf pts gauff point fr
    ,"système plus écologiques comme une pompe à chaleur" # france tirer les neuf pts gauff point fr
    ,"on regarde ensemble comment bénéficier de maprimerénov"
    ,"point de départ pour bien démarrer votre rénovation énergétique"
    ,"transition énergétique l' objectif c' est de réduire leurs émissions de co deux et leur budget énergie" # carrefour
    ,"énergie et notre avenir"
    ,"énergie est notre avenir"
    ,"totalenergies est le partenaire de notre transition énergétique"
    ,"déjà cent ans que toutes nos énergies" #totalenergies
    ,"total énergie je conseille à mes clients dans la transition énergétique"
    ,"nous progressons pour que la transition énergétique soit une réussite"
    ,"transition énergétique tout en les accompagnant"
    ,"total énergie"
    ,"option énergies vertes"
    ,"acteur majeur de la production d' énergie verte et de la gestion des déchets"
    ,"important que de passer à l' énergie verte tellement important"
    ,"important est de passer à l' énergie verte tellement important"
    ,"franchisés dans les énergies renouvelable" # verlaine
    ,"franchisé dans les énergies renouvelable" # verlaine
    ,"spécialiser dans les énergies renouvelable" # verlaine
    ,"énergies renouvelables renouvelables groupe verlaine"
    ,"franchisé dans les énergies les renouvelables énergies renouvelables"
    ,"produire sa propre énergie renouvelable et locale" #edf
    ,"l' objectif c' est de réduire ses émissions de co deux" #edf
    ,"c' était mission décarboner" # edf
    ,"décarbonation avec edf la décarbonation"
    ,"edf accompagne les entreprises dans leur décarbonation"
    ,"transition énergétique autour de trois types d' actions" #edf
    ,"edf propose des solutions adaptées à toutes les entreprises l' industrie"
    ,"écologiques pour remplacer votre"
    ,"accrochages c' est aussi plus écologique"
    ,"par l' ademe l' agence de la transition écologique avec le groupe tf1"
    ,"transition écologique avec le groupe tf1"
    ,"planète radio"
    ,"responsable service"
    ,"plus responsable chez électro dépôt"
    ,"haut responsable"
    ,"responsable politique"
    ,"vous êtes responsable"
    ,"responsable du"
    ,"responsables du"
    ,"la responsable"
    ,"le responsable"
    ,"les responsables"
    ,"chaque responsable"
    ,"responsable d'"
    ,"pas responsable"
    ,"autorités responsables"
    ,"épargne responsable banque populaire"
    ,"bactéries responsable"
    ,"virus responsable"
    ,"responsable commercial"
    ,"responsable de"
    ,"ce responsable"
    ,"responsables politiques"
    ,"responsable politique"
    ,"comme responsable"
    ,"du responsable"
    ,"verger écoresponsable"
    ,"responsable trois paires pour seulement deux"
    ,"français écoresponsable sont à"
    ,"spécialisée dans la formation des responsables de demain"
    ,"le monde agit pour un habitat plus responsable"
    ,"permettre de manger mieux et plus responsable en privilégiant des pratiques"
    ,"assurance vie responsable et solidaire" #maif
    ,"florence vie responsable et solidairee" #maif
    ,"érigés en barrage"
    ,"partie du barrage"
    ,"barrage au"
    ,"barrage et affrontement"
    ,"barrage à"
    ,"faire barrage"
    ,"barrage filtrant"
    ,"présents sur ce barrage"
    ,"barrages sur les routes"
    ,"barrage sur les routes"
    ,"barrages à signaler"
    ,"barrages d' agriculteurs"
    ,"barrages de la ligue"
    ,"barrage routier"
    ,"barrage de la police"
    ,"par terre"
    ,"pieds sur terre"
    ,"tremblement de terre"
    ,"paradis sur terre"
    ,"terre battue"
    ,"rituel de la forêt"
    ,"nouveau sa forêt"
    ,"sans parfum sa forêt"
    ,"passé par le jardin"
    ,"passés par le jardin"
    ,"jardin des tuileries"
    ,"jardins d' assainissement"
    ,"spécialiste de la maison du jardin"
    ,"électrique ou hydrogène il existe"
    ,"électrique hydrogène il existe"
    ,"skoda sur des véhicules électriques"
    ,"véhicule électrique dacia"
    ,"des véhicules électriques nouvelle génération"
    ,"achat d' un véhicule électrique hors frais"
    ,"entretenir les véhicules des pros et des particuliers"
    ,"kilomètres en camion biogaz et biocarburant" #colissimo
    ,"émerger la voix des biocarburants"
    ,"gaz renouvelables et de l' hydrogène gèrent gaz"
    ,"gaz renouvelables et de l' hydrogène vert et gaz"
    ,"nous transportons le gaz"
    ,"de l' ombre et de la lumière"
    ,"une ombre celle des violences"
    ,"vélo pour aller chercher le pain"
    ,"services auto moto camion vélo"
    ,"qu' est-ce qui vous fait aimer le vélo électrique"
    ,"services autos motos camions vélos"
    ,"camion ou du vélo"
    ,"vélo liddell"
    ,"kit solaire point solaire fr"
    ,"installer des panneaux solaires par exemple d' accord"
    ,"expert panne en panneaux solaires"
    ,"vélo décathlon"
    ,"vélo électrique de décathlon"
    ,"vélo électriques de elops"
    ,"vélo électrique nakamura"
    ,"on rachète votre ancien vélo"
    ,"ça marche aussi pour les vélos" # carglass
    ,"sofinco tu peux acheter un vélo électrique"
    ,"votre trajet à vélo un vrai"
    ,"c' est quoi un vélo électrique"
    ,"à vélo avec les forfaits kilométriques alliance"
    ,"vue sur mer"
    ,"issu de l' agriculture biologique en boutique"
    ,"agriculture biologique pour les produits de beauté"
    ,"vous aimez les voitures électriques" # renault
    ,"vous aimiez les voitures électriques"
    ,"les voitures de demain électrique"
    ,"excellente voitures électriques ou hybrides"
    ,"hybride sans recharge"
    ,"sans recharge"
    ,"rendre nos voitures cent pour cent électrique accessib"
    ,"renault fabrique et entretient des voitures électriques"
    ,"and roll des voitures électriques"
    ,"conduisiez une voiture électrique l' application"
    ,"engie pourquoi l' ordinaire devrait être la norme"
    ,"de plus en plus d' énergie éolien comme celui-ci" # amazon
    ,"énergie parc éolien comme celui-ci" # amazon
    ,"grand acheteur d' énergies renouvelable"
    ,"acheteur privé d' énergies renouvelable"
    ,"j' ai éteint je décale pour la planète"
    ,"experts en énergie en savoir plus"
    ,"je t' ai dit que j' avais les chauffe-eau"
    ,"de prix maman chauffage"
    ,"si on veut économiser plus énergie on peut aussi lever le pied"
    ,"jusqu' à quarante pour cent d' énergie"
    ,"co deux et leurs factures d' énergie edf"
    ,"entreprises sont des économies d' énergie ou l' impact environnemental des vies"
    ,"gaspilleurs dubois et de l' énergie où on peut bien se chauff" #ademe
    ,"gaspilleurs dubois et de l' énergie"
    ,"carrefour énergies pour l' installation d' une pompe"
    ,"retrouvez la météo avec carrefour énergie"
    ,"pompe à chaleur et faire des économies carrefour"
    ,"acteur majeur de la production d' énergies vertes et de la gestion des déchets"
    ,"énergies renouvelables au monde" # amazon
    ,"en privilégiant les énergies renouvelables pas le pétrole charbon"
    ,"financiers en privilégiant les énergies renouvelable" #maif
    ,"énergies vertes du génie"
    ,"pas le pétrole le charbon maif"
    ,"voiture ce soir parce qu' avec son assureur"
    ,"partenaire de vos économies d' énergie"
    ,"tous les financements pour l' énergie vont aux énergies renouvelables"
    ,"amazon notre planète"
    ,"l' eau filtrée britta"
    ,"brita l' eau filtrée"
    ,"britta l' eau filtrée"
    ,"bonne maman beaucoup de noisette huile de palme"
    ,"sans pesticides pour des recettes"
    ,"chez céréales bio"
    ,"agir pour la préserver découvrez comment" #gouv.fr
    ,"oasis de verdure au milieu"
    ,"feux de forêt elle sait soraya"
    ,"feux de forêt luc il fait un barbecue en forêt"
    ,"feux de forêt saint-luc il fait un barbecue en forêt"
    ,"produisait enfin votre propre énergie"
    ,"mettant en commun toutes nos énergies que nous avançons"
    ,"numériques au service des énergies vertes thématique de cette"
    ,"consommer votre propre énergie avec" #verlaine
    ,"consommez votre propre énergie avec" #verlaine
    ,"robinet pour économiser l' énergie"
    ,"souscrire un contrat d' énergie"
    ,"programme avec tuquoi énergie expert"
    ,"tucoy énergie"
    ,"option énergies vertes profitez d' une"
    ,"grâce à leur énergie leur engagement leur passion"
    ,"logement social énergies renouvelables chaque jour la banque"
    ,"économies d' énergie totalenergies"
    ,"énergie avec totalenergies"
    ,"total énergie"
    ,"co deux des transports avec totalenergies"
    ,"d' énergie avec total énergies"
    ," en main vos économies d' énergie"
    ,"ademe point fr ceci est un"
    ,"de la transition écologique de l' énergie du climat et de la prévention des ris"
    ,"message de l' ademe et du ministère de la transition écologique"
    ,"ceci est un message de l' ademe du ministère de la transition écologique"
    ,"ceci est un message à l'ademe et du ministère de la transition écologique"
    ,"ceci est un message à l' ademe"
    ,"ceci est un message de l' ademe"
    ,"ademe ceci est un message"
    ,"ademe fr ceci est un message"
    ,"ademe point fr"
    ,"point ademe fr"
    ,"ademe point fr"
    ,"ademe pourrait faire"
    ,"bien chauffé au bois point ademe"
    ,"financent la transition écologique les entreprises"
    ,"la transition écologique chef d' entreprise"
    ,"message du ministère de la transition écologique"
    ,"contribution recyclage offre valable"
    ,"contributions recyclage offre valable"
    ,"les joies du recyclage moi j' ai peur"
    ,"leader français du recyclage"
    ,"reconditionnés pour une alternative plus responsable chez électro"
    ,"reconditionné pour une alternative plus responsable chez électro"
    ,"l' ombre en préservant les arbres"
    ,"nos régions aujourd' hui les circuits courts"
    ,"stocker le carbone ou faire de l' ombre en préservant les arbres"
    ,"opter pour de l' énergie renouvelable cette option"
    ,"spécialisé dans les énergies renouvelables plus d' infos sur"
    ,"des pyrénées quelles énergies renouvelables bien évidemment"
    ,"se prélassait à l' ombre"
    ,"senseo et produits de façon plus durable plus responsable"
    ,"senseo et produits de façon plus durable et plus responsable"
    ,"prêt transition énergétique un prêt à taux réduit" # cic
    ,"engie soutient les pros dans leur transition énergétique"
    ,"fondation goodplanet soutient la transition agro écologique"
    ,"banque coopérative dédié à la transition écologique"
    ,"matériels professionnels climatiques retrouvez nos produits"
    ,"ce moment au pouvoir acheter une voiture électrique peut être une prise de"
    ,"pouvoir acheter une voiture électrique peut être"
    ,"pouvoir acheter une voiture électrique ça pouvait être une prise"
    ,"je dois faire réparer ma voiture rapidement"
    ,"pouvoir acheter une voiture électrique peut être une triste date"
    ,"connaître la valeur de votre voiture"
    ,"ça rentre vraiment la voiture électrique accessible oui mais fiat"
    ,"ça rendait vraiment la voiture électrique accessible oui mais fiat"
    ,"sud radio en voiture"
    ,"citroën c4 you c' est une voiture"
    ,"valeur de votre voiture vendez votre voiture point fr"
    ,"combien vaut votre voiture sur vendez votre voiture point fr"
    ,"révision de ma voiture électrique en plus ça me coûte la peau"
    ,"révision de la voiture électrique en plus ça me coûte la peau"
    ,"pare-brise de sa nouvelle voiture"
    ,"habitat et jardin"
    ,"habitat et jardins"
    ,"votre programme avec habitat et jardin"
    ,"entretenir facilement votre jardin"
    ,"chet ecomaison réemploi"
    ,"ecomaison réemploi et recycle"
    ,"ecomaison réemploi"
    ,"éhicule électrique la banque postale"
    ,"quotidien prenez les transports en commun"
    ,"trajets courts privilégiez la marche le vélo"
    ,"trajets courts privilégiez la marche ou le vélo"
    ,"remettre en état son vélo"
    ,"ça va plutôt vélo pour la sortie vélo"
    ,"comptoir de la mer point fr"
    ,"univers de la mer et au comptoir de la mer"
    ,"isolation thermique et phonique sécurité"
    ,"se lancer dans la décarbonation je leur propose expertise"
    ,"réparation point écho"
    ,"sa recharge permet de réduire de quatre-vingts pour cent son empreint"
    ,"sa recharge permet de réduire de quatre-vingt pourcent son empreint"
    ,"hors sol"
    ,"se concrétiser grâce à la forêt"
    ,"arrêts maladie"
    ,"arrêt maladie"
    ,"préserver mon karma"
    ,"pour les trajets courts finit la voiture"
    ,"pare-brise de sa nouvelle voiture"
    ,"norauto pour votre voiture"
    ,"voitures à vivre parlons de renault"
    ,"bienvenue dans nos prairies"
    ,"nectaire nectar des prairies"
    ,"recycler complètement la paire de lunettes"
    ,"collecte sur écosystème recycler ses protég"
    ,"écosystèmes font écho écosystème recycler"
    ,"écosystème point éco écosystème recycler"
    ,"écosystème font écho écosystème recycler"
    ,"pour le recyclage mon j"
    ,"tu vas connaître les joyeux recyclag"
    ,"thomas il a décidé d' être flexitariens car être flexitarien"
    ,"en choisissant en une choisissant viande une viande plus durable"
    ,"elle se demande celle de recharger sa voiture"
    ,"leur énergie vous aide avec ma recharge intelligente"
    ,"au gaz vert la france pourrait sortir france du pourrait gaz"
    ,"au gaz vert la france pourrait sortir du gaz"
    ,"renouvelable le gaz vert on est prêt à tout pour le faire connaître"
    ,"gaz vert on est prêt à tout pour le faire connaître"
    ,"leao l' équipe on a des on parle de décarboner"
    ,"parle de décarboner local et renouvelable le gaz vert"
    ,"parle de décarboner locale et renouvelable le gaz vert"
    ,"ce genre on parle de décarboner"
    ,"grâce au gaz vers la france pourrait sortir du gaz"
    ,"avec grdf quand on parle du gaz on note parfois la pompe à chaleur"
    ,"comparer les offres de gaz et d' électricité pour votre entreprise"
    ,"hors norme"
    ,"avant on pouvait demander au soleil de recharger vos batteries"
    ,"magnésium naturel"
    ,"responsable solutions énergétiques"
    ,"responsable ressources humaines"
    ,"outre-mer"
    ,"arme nucléaire"
    ,"ademe source ademe"
    ,"bois point ademe"
    ,"nouveau bouchon en plastique recyclé"
    ,"alexandre le mer"
    ,"grdf quand on parle du gaz on entre parfois dans le gaz et cent pour cent fossiles ben le gaz vert"
    ,"grdf quand on parle du gaz on entre parfois la pompe à chaleur"
    ,"grdf quand on parle du gaz on entre parfois assez"
    ,"quarante pour cent d' énergie consomm"
    ,"panneaux solaires avec edf solutions solaire"
    ,"un bidon ça se recharge encore et encore"
    ,"ça se recharge et avec le vrac"
    ,"encore et encore vrac recharge emballages et employables de réemploi"
    ,"fleuron industrie distributeur de panneaux solaire"
    ,"panneaux solaires fleuron industrie"
    ,"fleuron industrie"
    ,"rendez-vous avec elle est aussi l' efficacité énergétique"
    ,"rendez-vous avec elle et l' efficacité énergétique"
    ,"de son vivant"
]
