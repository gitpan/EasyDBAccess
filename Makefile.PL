require 5.004;
use ExtUtils::MakeMaker;

WriteMakefile(
	'NAME' => 'EasyDBAccess',
	'VERSION_FROM'	=>	'EasyDBAccess.pm',
	dist		=>	{ COMPRESS => 'gzip -9f', SUFFIX => '.gz'}
);

