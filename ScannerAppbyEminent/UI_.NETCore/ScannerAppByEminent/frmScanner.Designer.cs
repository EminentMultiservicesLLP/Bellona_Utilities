namespace ScannerApp
{
    partial class frmScanner
    {
        /// <summary>
        ///  Required designer variable.
        /// </summary>
        private System.ComponentModel.IContainer components = null;

        /// <summary>
        ///  Clean up any resources being used.
        /// </summary>
        /// <param name="disposing">true if managed resources should be disposed; otherwise, false.</param>
        protected override void Dispose(bool disposing)
        {
            if (disposing && (components != null))
            {
                components.Dispose();
            }
            base.Dispose(disposing);
        }

        #region Windows Form Designer generated code

        /// <summary>
        ///  Required method for Designer support - do not modify
        ///  the contents of this method with the code editor.
        /// </summary>
        private void InitializeComponent()
        {
            cmbScanners = new ComboBox();
            label1 = new Label();
            btnRefresh = new Button();
            btnScan = new Button();
            progressTextBox = new RichTextBox();
            chkDuplexScan = new CheckBox();
            SuspendLayout();
            // 
            // cmbScanners
            // 
            cmbScanners.DropDownStyle = ComboBoxStyle.DropDownList;
            cmbScanners.FormattingEnabled = true;
            cmbScanners.Location = new Point(12, 37);
            cmbScanners.Name = "cmbScanners";
            cmbScanners.Size = new Size(264, 28);
            cmbScanners.TabIndex = 0;
            // 
            // label1
            // 
            label1.AutoSize = true;
            label1.Location = new Point(12, 14);
            label1.Name = "label1";
            label1.Size = new Size(133, 20);
            label1.TabIndex = 1;
            label1.Text = "Available Scanners";
            // 
            // btnRefresh
            // 
            btnRefresh.FlatStyle = FlatStyle.Popup;
            btnRefresh.Font = new Font("Segoe UI", 9F, FontStyle.Bold);
            btnRefresh.Image = ScannerAppByEminent.Properties.Resources.refresh_24;
            btnRefresh.ImageAlign = ContentAlignment.MiddleLeft;
            btnRefresh.Location = new Point(282, 30);
            btnRefresh.Name = "btnRefresh";
            btnRefresh.Size = new Size(93, 38);
            btnRefresh.TabIndex = 2;
            btnRefresh.Text = "Refresh";
            btnRefresh.TextAlign = ContentAlignment.MiddleRight;
            btnRefresh.UseVisualStyleBackColor = true;
            btnRefresh.Click += btnRefresh_Click;
            // 
            // btnScan
            // 
            btnScan.FlatStyle = FlatStyle.Popup;
            btnScan.Font = new Font("Segoe UI", 9F, FontStyle.Bold);
            btnScan.Image = ScannerAppByEminent.Properties.Resources.play_24;
            btnScan.ImageAlign = ContentAlignment.MiddleLeft;
            btnScan.Location = new Point(282, 72);
            btnScan.Name = "btnScan";
            btnScan.Size = new Size(93, 38);
            btnScan.TabIndex = 3;
            btnScan.Text = "Scan";
            btnScan.TextAlign = ContentAlignment.MiddleRight;
            btnScan.UseVisualStyleBackColor = true;
            btnScan.Click += btnScan_Click;
            // 
            // progressTextBox
            // 
            progressTextBox.Location = new Point(12, 114);
            progressTextBox.Name = "progressTextBox";
            progressTextBox.Size = new Size(845, 211);
            progressTextBox.TabIndex = 4;
            progressTextBox.Text = "";
            // 
            // chkDuplexScan
            // 
            chkDuplexScan.AutoSize = true;
            chkDuplexScan.Location = new Point(13, 71);
            chkDuplexScan.Name = "chkDuplexScan";
            chkDuplexScan.Size = new Size(225, 24);
            chkDuplexScan.TabIndex = 5;
            chkDuplexScan.Text = "Need both side scan (duplex)";
            chkDuplexScan.UseVisualStyleBackColor = true;
            // 
            // frmScanner
            // 
            AutoScaleDimensions = new SizeF(8F, 20F);
            AutoScaleMode = AutoScaleMode.Font;
            ClientSize = new Size(860, 333);
            Controls.Add(chkDuplexScan);
            Controls.Add(progressTextBox);
            Controls.Add(btnScan);
            Controls.Add(btnRefresh);
            Controls.Add(label1);
            Controls.Add(cmbScanners);
            Name = "frmScanner";
            Text = "Scanner App by Eminent";
            Load += frmScanner_Load;
            ResumeLayout(false);
            PerformLayout();
        }

        #endregion

        private ComboBox cmbScanners;
        private Label label1;
        private Button btnRefresh;
        private Button btnScan;
        private RichTextBox progressTextBox;
        private CheckBox chkDuplexScan;
    }
}
