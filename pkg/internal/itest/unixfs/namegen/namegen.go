package namegen

import (
	"encoding/binary"
	"io"
	"strings"
)

var words = strings.Fields(wordData)
var extensions = []string{"", ".txt", ".pdf", ".docx", ".png", ".jpg", ".csv", ".json", ".xml"}

func getRandomIndex(r io.Reader, max int) (int, error) {
	var n uint32
	err := binary.Read(r, binary.BigEndian, &n)
	if err != nil {
		return 0, err
	}
	return int(n % uint32(max)), nil
}

// RandomDirectoryName returns a random directory name from the provided word list.
func RandomDirectoryName(r io.Reader) (string, error) {
	index, err := getRandomIndex(r, len(words))
	if err != nil {
		return "", err
	}
	return words[index], nil
}

// RandomFileName returns a random file name with an extension from the provided word list and common extensions.
func RandomFileName(r io.Reader) (string, error) {
	wordIndex, err := getRandomIndex(r, len(words))
	if err != nil {
		return "", err
	}
	extIndex, err := getRandomIndex(r, len(extensions))
	if err != nil {
		return "", err
	}
	return words[wordIndex] + extensions[extIndex], nil
}

const wordData = `jabberwocky Snark whiffling borogoves mome raths brillig slithy toves outgrabe
Tumtum Frabjous Bandersnatch Jubjub Callay slumgullion snicker-snack brobdingnagian Jabberwock
tree Poglorian Binkleborf Wockbristle Zizzotether dinglewock Flumgurgle Glimperwick RazzleDazzle8
gyre tortlewhack whispyfangle Crumplehorn Higgledy7 Piggledy3 flibberwocky Zamborot Flizzleflink
gimble Shakespearean Macbeth Othello Hamlet soliloquy iambic pentameter Benvolio Capulet Montague
Puck Malvolio Beatrice Prospero Iago Falstaff Rosencrantz Guildenstern Cordelia Polonius
Titania Oberon Tybalt Caliban Mercutio Portia Brabantio 4Lear Desdemona Lysander
YossarianScar Jujimufu9 Gorgulon Oozyboozle Razzmatazz8 BlinkenWoggle Flibbertigibbet Quixotic2
Galumphing Widdershins Pecksniffian Bandicoot11 Flapdoodle Fandango Whippersnapper Grandiloquent
Lollygag Persnickety Gibberish Codswallop Rigmarole Nincompoop Flummox Snollygoster Poppycock
Kerfuffle Balderdash Gobbledygook Fiddle-faddle Antidisestablishmentarianism
Supercalifragilisticexpialidocious Rambunctious9 Lickety-split Hullabaloo Skullduggery Ballyhoo
Flabbergasted Discombobulate Pernicious Bumfuzzle Bamboozle Pandemonium Tomfoolery Hobbledehoy7
Claptrap Cockamamie Hocus-pocus8 Higgledy-piggledy Dodecahedron Nonsensical Contraption Quizzical
Snuffleupagus Ostentatious Serendipity Ephemeral Melancholy Sonorous Plethora Brouhaha Absquatulate
Gobbledygook3 Lilliputian Chortle Euphonious Mellifluous Obfuscate Perspicacious Prevaricate
Sesquipedalian Tintinnabulation Quibble9 Umbrageous Quotidian Flapdoodle5 NoodleDoodle
Zigzagumptious Throttlebottom WuzzleWump Canoodle Hodgepodge Blatherskite7 Hornswoggle
BibbidiBobbidiBoo Prestidigitation Confabulate Abscond8 Lickspittle Ragamuffin Taradiddle
Widdershins4 Boondoggle Snuffleupagus9 Gallivant Folderol Malarkey Skedaddle Hobgoblin
BlubberCrumble ZibberZap Snickerdoodle Mooncalf LicketySplit8 Whatchamacallit Thingamajig
Thingamabob GibbleGabble FuddleDuddle LoopyLoo Splendiferous Bumbershoot Catawampus Flibbertigibbet5
Gobbledygook7 Whippersnapper9 Ragamuffin8 Splendiferous
ætheling witan ealdorman leofwyrd swain bēorhall beorn mēarh scōp cyning hēahgerefa
sceadugenga wilweorc hildoræswa þegn ælfscyne wyrmslaga wælwulf fyrd hrēowmōd dēor
ealdorleornung scyldwiga þēodcwealm hāligbōc gūþweard wealdend gāstcynn wīfmann
wīsestōw þrēatung rīcere scealc eorþwerod bealucræft cynerīce sceorp ættwer
gāsthof ealdrīce wæpnedmann wæterfōr landgemære gafolgelda wīcstede mægenþrymm
æscwiga læcedōm wīdferhþ eorlgestrēon brimrād wæterstede hūslēoþ searocraeft
þegnunga wælscenc þrīstguma fyrdrinc wundorcræft cræftleornung eorþbūend
sǣlācend þunorrad wætergifu wæterscipe wæterþenung eorþtilþ eorþgebyrde
eorþhæbbend eorþgræf eorþbærn eorþhūs eorþscearu eorþsweg eorþtæfl eorþweorc
eorþweall eorþwaru eorþwela eorþwīs eorþworn eorþyþ eorþweg eorþwīse eorþwyrhta
eorþwīn eorþsceaða eorþsweart eorþscræf eorþscrūd eorþswyft eorþscīr eorþscūa
eorþsēoc eorþsele eorþhūsl eorþsted eorþswyn eorþsittend eorþsniþ eorþscearp
eorþscyld eorþsceaft eorþstapol eorþstede eorþsmitta eorþscēawere
velociraptorious chimeraesque bellerophontic serendipitastic transmogrification ultracrepidarian
prestidigitationary supraluminescence hemidemisemiquaver unquestionability intercontinentalism
antediluvianistic disproportionately absquatulationism automagicalization
floccinaucinihilipilification quintessentiality incomprehensibility juxtapositionally
perpendicularitude transubstantiation synchronicityverse astronomicalunit thermodynamicness
electromagnetismal procrastinatorily disenfranchisement neutrinooscillation hyperventilatingly
pneumonoultramicroscopicsilicovolcanoconiosis supercalifragilisticexpialidocious thaumaturgeonomics
idiosyncratically unencumberedness phantasmagoricity extraterrestrialism philanthropistastic
xenotransplantation incontrovertibility spontaneityvolution teleportationally labyrinthinean
megalomaniaction cryptozoologician ineffablemystique multiplicativity sisypheanquandary
overenthusiastically irrefutablenotion exceptionalitysphere
blibby ploof twindle zibbet jinty wiblo glimsy snaft trindle quopp vistly chark plizet snibber frint
trazzle buvvy skipple flizz dworp grindle yipple zarfle clippet swazz mibber brackle tindle grozz
vindle plazz freggle twazz snuzzle gwippet whindle juzzle krazz yazzle flippet skindle zapple prazz
buzzle chazz gripple snozzle trizz wazzle blikket zib glup snof yipr tazz vlim frub dwex klop
aa ab ad ae ag ah ai al am an as at aw ax ay ba be bi bo by de do ed ef eh el em en er es et ex fa
fe go ha he hi hm ho id if in is it jo ka ki la li lo ma me mi mm mo mu my na ne no nu od oe of oh
oi om on op or os ow ox oy pa pe pi qi re sh si so ta ti to uh um un up us ut we wo xi xu ya ye yo
za zo
hĕlłø cąfѐ ŝmîłe þřęê ċỏẽxist ǩāŕáōķê ŧrävèl кυгiοsity ŭпịςørn мëĺōđỳ ğħōšţ ŵăνę ẓẽṕhýr ғụzzlę
пåŕŧy åƒƒêct ԁяêåм љúвïĺëë ѓåḿъḽë ţęmƿęşţ říše čajovna želva štěstí ýpsilon ďábel ňadraží ťava
h3ll0 w0rld c0d1ng 3x3mpl3 pr0gr4mm1ng d3v3l0p3r 5cr4bbl3 3l3ph4nt 4pp 5y5t3m 1nput 0utput 3rr0r
5t4ck0v3rfl0w 5tr1ng 5l1c3 5h4k35p34r3 5t4nd4rd 3ncrypt10n 5h3ll 5cr1pt 5t4ck 5qu4r3 r3ct4ngl3
tr14ngl3 c1rc13 5ph3r3 5qu4r3r00t 3xpr35510n 5t4t15t1c5 5t4t3m3nt 5ynt4x 5ugg35t10n 5y5t3m4t1c
5h0rtcut 5h4d0w 5h4r3d
1 2 3 4 5 6 7 8 9 0
a b c d e f g h i j k l m n o p q r s t u v w x y z
A B C D E F G H I J K L M N O P Q R S T U V W X Y Z`
