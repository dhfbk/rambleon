$temp_file=$file;
$section=" ";
$subsection=" ";
$text=" ";

while(<>){
if($_=~/<h2><span class.+?>(.+?)<\/span>/){
	$section=$1;
	$section=~s/<a href.+?>(.+?)<\/a>/$1/g;
	$section=~s/<sup id.+?class="reference">.+?<\/sup>//g;
	$section=~s/<.+?>//g;
	$section=~s/\"//g;
	$subsection=" ";
	}
if($_=~/<h3><span class.+?>(.+?)<\/span>/){
	$subsection=$1;
	$subsection=~s/<a href.+?>(.+?)<\/a>/$1/g;
	$subsection=~s/<sup id.+?class="reference">.+?<\/sup>//g;
	$subsection=~s/<.+?>//g;
	$subsection=~s/\"//g;
	}
if($_=~/<p>(.+?)<\/p>/){
	$text=$1;
	$text=~s/<a href.+?>(.+?)<\/a>/$1/g;
	$text=~s/<sup.+?>.+?<\/sup>//g;

	$text=~s/<.+?>//g;
	unless ($section=~/(See also|Publications|References|Further reading|External links|Notes)/ || $text=~/^[0-9]+$/){
		$text=~s/\&\#160;/ /g;
		print  $text."\n";
		$id++;
		}
	}
}



