#!/usr/bin/env python

import sys
from distutils import spawn
import subprocess
import re
import shutil
import os
import os.path


class Link:
    def __init__(self, label, md_file):
        self.label = label
        self.md_file = md_file

    def md_file_abs_path(self, input_dir):
        return os.path.join(input_dir, self.md_file)

    def html_file(self):
        return re.sub(r"\.md$", ".html", self.md_file)

    def html_file_abs_path(self, output_dir):
        return os.path.join(output_dir, self.html_file())

    def __str__(self):
        return "Link<%s,%s>" % (self.label, self.md_file)

    __repr__ = __str__

def dummyLink():
    return Link("", "_dummy_.md")

class CmdRunner:
    """Helper class to aid with running shell commands for generating the report"""
    def __init__(self, argv):
        self.argv = argv
        self._setSmvPaths()
        self._confirm_pandoc()

    def _setSmvPaths(self):
        self.smv_tools = os.path.abspath(os.path.dirname(self.argv[0]))
        self.smv_home = os.path.abspath(os.path.join(self.smv_tools, os.pardir))

    def _confirm_pandoc(self):
        """confirm that we have pandoc command in our path"""
        pandoc_path = spawn.find_executable("pandoc")
        if not pandoc_path:
            print("Can not find pandoc executable")
            sys.exit(1)

    def _sub_smv_paths(self, s):
        """return s with @SMV_HOME and @SMV_TOOLS substituted in"""
        s = s.replace("@SMV_HOME", self.smv_home)
        s = s.replace("@SMV_TOOLS", self.smv_tools)
        return s

    def run_cmd(self, cmd):
        """Run the given cmd and substitute the true SMV home dir for @SMV_HOME, and smv tools dir for @SMV_TOOLS"""
        cmd = cmd.replace("@SMV_HOME", self.smv_home)
        cmd = cmd.replace("@SMV_TOOLS", self.smv_tools)
        # print "Run:", cmd
        return subprocess.check_output(cmd, shell=True)

    def mkdir(self, dirname):
        """make a directory and all parent paths (similar to mkdir -p in shell)"""
        try:
            os.makedirs(dirname)
        except OSError:
            if os.path.isdir(dirname):
                pass
            else:
                raise

    def copy(self, src, dst):
        """copy src file to dst file/dir with smv path substitution"""
        src = self._sub_smv_paths(src)
        dst = self._sub_smv_paths(dst)
        shutil.copy(src, dst)


class DocGenerator:
    """Generate the HTML document/book from the starting Markdown TOC file"""
    def __init__(self, argv):
        self._parse_args(argv)
        self.runner = CmdRunner(argv)

    def _parse_args(self, argv):
        if len(argv) != 3:
            print "USAGE: %s md_toc_file output_dir" % (argv[0],)
            sys.exit(1)
        self.md_toc_file = argv[1]
        self.output_dir = argv[2]
        self.md_input_dir = os.path.abspath(os.path.dirname(self.md_toc_file))

    def _getTocLinks(self):
        """get links found in markdown TOC file"""
        md_toc_str = open(self.md_toc_file).read()
        link_re = re.compile(r"\[([^\]]+)\]\(([^\)]+)\)")
        links = []
        for m in link_re.finditer(md_toc_str):
            links.append(Link(m.group(1), m.group(2)))
        return links

    def _filterValidLinks(self, links):
        """filter sequence of links to remove any links with invalid paths"""
        return [l for l in links if
                os.path.exists(l.md_file_abs_path(self.md_input_dir))]

    def _writeFile(self, path, content):
        fp = open(path, "w")
        fp.write(content)
        fp.close()

    def _fillNavigationLinks(self, html, prev_link, next_link):
        """fill in the navigation links at the top of the generated html content"""
        d = {
            "@PREV_LINK": prev_link.html_file(),
            "@PREV_LABEL": prev_link.label,
            "@NEXT_LINK": next_link.html_file(),
            "@NEXT_LABEL": next_link.label,
            "@TOC_LINK": self._dummyTocLink().html_file()
        }
        for (key, value) in d.items():
            html = html.replace(key, value)
        return html

    def _removeTableLineBreaks(self, html):
        """remove line breaks <br /> inserted after every table element by geneator"""
        tags = ["<table>", "</table>", "<tr>", "</tr>", "<th>", "</th>", "<td>", "</td>"]
        for t in tags:
            html = html.replace(t + "<br />", t)
        return html

    def _markdownToHtml(self, link, prev_link, next_link, useHeaderWithLinks=True):
        """convert given markdown file to html (in the specified output directory)"""
        print "convert file to html:", link.md_file
        md_abs_path = link.md_file_abs_path(self.md_input_dir)
        html_abs_path = link.html_file_abs_path(self.output_dir)

        if useHeaderWithLinks:
            prefix_file = "body_before.html"
        else:
            prefix_file = "body_before_no_nav.html"

        cmd = r"""
            pandoc -s -f markdown_github -t html \
                "--include-in-header=@SMV_HOME/tools/conf/docs/header.html" \
                "--include-before-body=@SMV_HOME/tools/conf/docs/%s" \
                "--include-after-body=@SMV_HOME/tools/conf/docs/body_after.html" \
                --normalize \
                -o - "%s" |\
            sed -e 's/\(href="[^"]*\)\.md"/\1.html"/g'
            """ % (prefix_file, md_abs_path)

        output = self.runner.run_cmd(cmd)
        output = self._fillNavigationLinks(output, prev_link, next_link)
        output = self._removeTableLineBreaks(output)
        self._writeFile(html_abs_path, output)

    def _dummyTocLink(self):
        """create a dummy link to the toc file."""
        md_basename = os.path.basename(self.md_toc_file)
        return Link("toc", md_basename)

    def _tocToHtml(self):
        """convert the input toc markdown file to html"""
        self._markdownToHtml(self._dummyTocLink(), dummyLink(), dummyLink(), False)

    def _copyGithubCss(self):
        # This used https://github.com/sindresorhus/generate-github-markdown-css to get the github.css file
        self.runner.copy("@SMV_TOOLS/conf/docs/github.css", self.output_dir)

    def _getNthLink(self, links, idx):
        """return the nth link in the list of links or a dummy link if idx is out of range."""
        if idx < 0 or idx >= len(links):
            return dummyLink()
        return links[idx]

    def generate(self):
        self.runner.mkdir(self.output_dir)
        links = self._getTocLinks()
        valid_links = self._filterValidLinks(links)
        for idx, link in enumerate(valid_links):
            prev_link = self._getNthLink(valid_links, idx-1)
            next_link = self._getNthLink(valid_links, idx+1)
            self._markdownToHtml(link, prev_link, next_link)
        self._tocToHtml()
        self._copyGithubCss()


# ---- MAIN ----
dg = DocGenerator(sys.argv)
dg.generate()
