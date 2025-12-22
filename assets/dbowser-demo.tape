Output "./assets/dbowser-demo.gif"

Require dbowser

Set Shell "zsh"
Set FontSize 20
Set Width 1600
Set Height 1000
Set Theme "Dracula"
Set TypingSpeed 15ms

Hide
Type "rm -rf .vhs-home" Enter
Type "mkdir -p .vhs-home" Enter
Type "PROMPT='$ ' RPROMPT='' PS1='$ ' HOME=$PWD/.vhs-home zsh -f" Enter
Type "clear" Enter
Show

Type "dbowser add-connection --name local --url postgresql://dbowser:dbowser@localhost:54329/dbowser_test" Enter
Sleep 300ms
Type "dbowser" Enter

Sleep 1s
Type "a"
Sleep 500ms
Type "new-local" Enter
Sleep 300ms
Type "postgresql://dbowser:dbowser@localhost:54329/dbowser_test" Enter
Sleep 900ms
Type "/new"
Sleep 400ms
Enter
Sleep 900ms
Down
Enter
Sleep 350ms
Enter
Sleep 900ms
Type "G"
Sleep 300ms
Enter
Type "/wid" Enter
Sleep 400ms
Type "G" Enter
Sleep 1500ms
Type "w"
Sleep 300ms
Type "quantity > 0" Enter
Sleep 700ms
Type "o"
Sleep 300ms
Type "quantity desc" Enter
Sleep 900ms
Type "v"
Right
Down
Sleep 900ms
Type "v"
Sleep 600ms
Escape
Sleep 600ms
Type ":query" Enter
Sleep 600ms
Type "r"
Sleep 900ms
Type ":q" Enter
